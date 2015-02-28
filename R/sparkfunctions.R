doDebug <- FALSE
reduction <- "NONE"
# processes documents.rdd, running logisticnormal and 
# producing a new documents.rdd with an updated lambda
estep.lambda <- function( 
  documents.rdd,
  beta.distributed,
  mu.distributed, 
  siginv.broadcast,
  spark.context,
  spark.partitions,
  verbose) {
  
  mapPartitionsWithIndex(documents.rdd, function(split, part) {   
    mu <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    siginv <- value(siginv.broadcast)
    llply(.data = part, .fun = function(listElement) {
      document <- listElement
      init <- document$l
      if (is.null(document$nd)) {
        document$nd <- sum(document$d[2,])
      }
      beta.i.lambda <- beta.in[[document$a]][,document$d[1,],drop=FALSE]
      if (ncol(mu) > 1) {
        mu.i <- mu[,document$dn]
      } else {
        mu.i <- as.numeric(mu)
      }
      
      document$l <- logisticnormal.lambda(eta = init, 
                                          mu = mu.i, 
                                          siginv = siginv,
                                          beta = beta.i.lambda, 
                                          doc.ct = document$d[2,], 
                                          Ndoc = document$nd
      )
      document
    }
    )
  })
}

logisticnormal.lambda <- function(eta, mu, siginv, beta, doc.ct, Ndoc) {
  optim.out <- optim(par=eta, fn=stm:::lhood, gr=stm:::grad,
                     method="BFGS", control=list(maxit=500),
                     doc.ct=doc.ct, mu=mu,
                     siginv=siginv, beta=beta, Ndoc=Ndoc)
  optim.out$par
}

# maps and reduces documents.rdd to collect sufficient stats
estep.hpb <- function( 
  V, 
  K,
  A,
  documents.rdd,
  beta.distributed,
  mu.distributed, 
  siginv.broadcast,
  sigmaentropy.broadcast,
  spark.context,
  spark.partitions,
  verbose) {
  
  # loops through partitions of documents.rdd, collecting sufficient stats per-partition.   Produces
  # a pair (key, value) RDD where the key is the partition and the value the sufficient stats.
  part.rdd <- mapPartitionsWithIndex(documents.rdd, function(split, part) {
    beta.ss <- vector(mode="list", length=A)
    for(i in 1:A) {
      beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
    }
    sigma.ss <- diag(0, nrow=(K-1))
    lambda <- rep(NULL, times = K) # K - 1, plus 1 column for row order so we can sort later
    
    mu <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    siginv <- value(siginv.broadcast)
    sigmaentropy <- value(sigmaentropy.broadcast)
    
    bound <- laply(part, .fun = function(document) {
      eta <- document$l
      words <- document$d[1,]
      if (ncol(mu) > 1) {
        mu.i <- mu[,document$dn]
      } else {
        mu.i <- as.numeric(mu)
      }
      
      #Solve for Hessian/Phi/Bound returning the result
      doc.results <- stm:::hpb(document$l, doc.ct=document$d[2,], mu=mu.i,
                               siginv=siginv, beta=beta.in[[document$a]][,words,drop=FALSE], document$nd ,
                               sigmaentropy=sigmaentropy)
      
      beta.ss[[document$a]][,words] <<- doc.results$phis + beta.ss[[document$a]][,words]
      sigma.ss <<- sigma.ss + doc.results$eta$nu
      lambda <<- rbind(lambda, c(document$dn, document$l))
      c(document$dn, doc.results$bound)
    })
    index <- as.integer(split/sqrt(spark.partitions))
    list(index, list(s = sigma.ss, 
                     b = beta.ss, 
                     bd = bound,
                     l = lambda
    ))
  })
  # try to combine using an intermediate step
  if ("KEY" %in% reduction) { # turn this on when we understand it better
    print("reduce by key")
    part.rdd <- reduceByKey(part.rdd, function(x, y) {
      if ((is.null(x) || is.integer(x)) && !is.null(y)) return(y)
      if ((is.null(y) || is.integer(y)) && !is.null(x)) return(x)
      if (length(x) == 4 && length(y) == 4) {
        list(bd = rbind(x$bd, y$bd), 
             s = x$s + y$s, 
             b = merge.beta(x$b, y$b), 
             l = rbind(x$l, y$l)
        )
      } else { 
        error(paste("bad reduction match",
                    str(x),
                    str(y))
        )
      }
    }, 
    numPartitions = as.integer(sqrt(spark.partitions))
    )
    print("Done reducing by key")
  }
  if ("COMBINE" %in% reduction) {
      print("combining")
      part.rdd <- combineByKey(part.rdd, createCombiner = function(v) {v}, 
                                function(C, v) {
        list(bd = rbind(C$bd, v$bd), 
             s = C$s + v$s, 
             b = merge.beta(C$b, v$b), 
             l = rbind(C$l, v$l)
        )
    },
    function(C1, C2) {
      list(bd = rbind(C1$bd, C2$bd), 
           s = C1$s + C2$s, 
           b = merge.beta(C1$b, C2$b), 
           l = rbind(C1$l, C2$l)
      )
    },
    numPartitions = as.integer(sqrt(spark.partitions))
    )
      print("done combining")
  }
  if ("COLLECT" %in% reduction) {
    print("Collecting")
    toss <- collect(part.rdd)
    print("Done collecting")
  }
  if ("COLLECTPARTITION" %in% reduction) {
    print("collecting partitions - counting")
    j <- numPartitions(part.rdd)
    for (i in 1:j) {
      print(i)
      toss <- collectPartition(part.rdd, as.integer(i))
    }
    print("collected")
  }
  # merge the sufficient stats generated for each partition
  print("final reduction")
  out <- reduce(part.rdd, function(x, y) {
    if ((is.null(x) || is.integer(x)) && !is.null(y)) return(y)
    if ((is.null(y) || is.integer(y)) && !is.null(x)) return(x)
    if (length(x) == 4 && length(y) == 4) {
      list(bd = rbind(x$bd, y$bd), 
           s = x$s + y$s, 
           b = merge.beta(x$b, y$b), 
           l = rbind(x$l, y$l)
      )
    } else { 
      error(paste("bad reduction match",
                  str(x),
                  str(y))
      )
    }
  })
  bound.ss <- out$bd[order(out$bd[,1]),]
  out$bd <- bound.ss[,2]
  lambda <- out$l[order(out$l[,1]),]
  out$l <- lambda[,-1]
  out
}

merge.beta <- function(x, y) {
  for (i in 1:length(x)) x[[i]] <- x[[i]] + y[[i]]
  x
}

distribute.beta <- function(beta, spark.context, spark.partitions) {
  broadcast(sc = spark.context, beta)
}

distribute.mu <- function(mu, spark.context, spark.partitions) {
  mu <- mu$mu
  broadcast(spark.context, mu)#)
}

mnreg.spark <- function(beta.ss,settings, spark.context, spark.partitions) {
  #Parse Arguments
  A <- settings$dim$A
  K <- settings$dim$K
  V <- settings$dim$V
  interact <- settings$kappa$interactions
  fixedintercept <- settings$kappa$fixedintercept
  alpha <- settings$tau$enet
  maxit <- settings$tau$maxit 
  nlambda <- settings$tau$nlambda
  lambda.min.ratio <- settings$tau$lambda.min.ratio
  ic.k <- settings$tau$ic.k
  thresh <- settings$tau$tol
  #Aggregate outcome data.
  
  counts <- do.call(rbind,beta.ss)
  
  covar.broadcast <- settings$covar.broadcast
  
  if(fixedintercept) {  
    m <- settings$dim$wcounts$x
    m <- log(m) - log(sum(m))
  } else {
    m <- NULL #have to assign this to null to keep code simpler below
  }
  mult.nobs <- rowSums(counts) #number of multinomial draws in the sample
  offset <- log(mult.nobs)
  offset.broadcast <- broadcast(spark.context, offset)
  
  # it is cheaper to re-distribute counts as an RDD each pass through the loop than it would be to 
  # convert the matrix from rows to columns.  This is something potentially worth revisiting
  # for very large data sets.
  counts <- split(counts, col(counts)) # now a list, indexed by term, of arrays
  index <- 0
  counts.list <- llply(counts, function(x) {
    index <<- index + 1
    list(
      t = index, 
      c.i = x, 
      m.i = ifelse(is.null(m), NULL, m[index])
    )
  })
  counts.rdd <- parallelize(spark.context, counts.list, spark.partitions)
  rm(counts.list)
  
  #########
  #Distributed Poissons
  #########
  
  #methods dispatch for S4 is crazy expensive so let's first define a function
  #for quickly extracting the coefficients from the model.
  subM <- function(x, p) {
    ind <- (x@p[p]+1):x@p[p+1]
    rn <- x@i[ind]+1
    y <- x@x[ind]
    out <- rep(0, length=nrow(x))
    out[rn] <- y
    out
  }
  
  verbose <- settings$verbose
  
  mnreg.rdd <- mapPartitionsWithIndex(counts.rdd, function(split, part) {
    # each part should be a column from counts, representing a single vocabulary term
    offset.in <- value(offset.broadcast)
    covar <- value(covar.broadcast)
    out <- laply(part, .fun = function(a.count) {
      i <- a.count$t
      counts.i <- a.count$c.i
      if (is.null(a.count$m.i)) {
        offset2 <- offset.in
      } else {
        offset2 <- a.count$m.i + offset.in
      }
      
      mod <- NULL
      while(is.null(mod)) {
        mod <- tryCatch(glmnet(x=covar, y=counts.i, family="poisson", 
                               offset=offset2, standardize=FALSE,
                               intercept=is.null(a.count$m.i), 
                               lambda.min.ratio=lambda.min.ratio,
                               nlambda=nlambda, alpha=alpha,
                               maxit=maxit, thresh=thresh),
                        warning=function(w) return(NULL),
                        error=function(e) stop(e))
        #if it didn't converge, increase nlambda paths by 20% 
        if(is.null(mod)) nlambda <- nlambda + floor(.2*nlambda)
      }
      dev <- (1-mod$dev.ratio)*mod$nulldev
      ic <- dev + ic.k*mod$df
      lambda <- which.min(ic)
      coef <- subM(mod$beta,lambda) #return coefficients
      if(is.null(a.count$m.i)) coef <- c(mod$a0[lambda], coef)
      c(i, coef)
    } )
    list(value = out)
  }
  )
  
  coef <- reduce(mnreg.rdd, function(x,y)  {   
    if ((is.null(x) || is.integer(x)) && !is.null(y)) return(y)
    if ((is.null(y) || is.integer(y)) && !is.null(x)) return(x)
    rbind(x, y)
  })
  coef <- t(coef)
  coef <- coef[,order(coef[1,])]
  coef <- coef[-1,]
  
  if(!fixedintercept) {
    #if we estimated the intercept add it in
    m <- coef[1,] 
    coef <- coef[-1,]
  }
  
  kappa <- split(coef, row(coef)) 
  ##
  #predictions 
  ##
  #linear predictor
  covar <- settings$covar
  
  linpred <- as.matrix(covar%*%coef) 
  
  linpred <- sweep(linpred, 2, STATS=m, FUN="+")
  #softmax
  explinpred <- exp(linpred)
  
  beta <- explinpred/rowSums(explinpred)
  
  beta <- split(beta, rep(1:A, each=K))
  
  #wrangle into the list structure
  beta <- lapply(beta, matrix, nrow=K)
  beta.distributed <- distribute.beta(spark.context = spark.context, beta, spark.partitions)
  
  kappa <- list(m=m, params=kappa)
  list(beta = beta, kappa=kappa, nlambda=nlambda, beta.distributed = beta.distributed)
}

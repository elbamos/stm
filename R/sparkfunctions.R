doDebug <- FALSE
#reduction <- "NONE"
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
 
    mu.in <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    siginv.in <- value(siginv.broadcast)

    lapply(part, function(document) {
      init <- document$l
      if (is.null(document$nd)) {
        document$nd <- sum(document$d[2,])
      }
      beta.i.lambda <- beta.in[[document$a]][,document$d[1,],drop=FALSE]
      if (ncol(mu.in) > 1) {
        mu.i <- mu.in[,document$dn]
      } else {
        mu.i <- as.numeric(mu.in)
      }
      document$l <- optim(par=init, fn=lhood, gr=grad,
                         method="BFGS", control=list(maxit=500),
                         doc.ct=document$d[2,], mu=mu.i,
                         siginv=siginv.in, beta=beta.i.lambda, Ndoc = document$nd)$par
      document
    }
    )
  })
}

lhood <- function(eta, doc.ct, mu, siginv,beta, Ndoc) {
  expeta <- c(exp(eta),1)
  part1 <- sum(doc.ct*log(.colSums(beta*expeta, nrow(beta), ncol(beta)))) - Ndoc*log(sum(expeta))
  # -1/2 (eta - mu)^T Sigma (eta - mu)
  diff <- eta-mu
  part2 <- .5*sum(diff*(siginv %*% diff))
  part2 - part1  
} 

grad <- function(eta, doc.ct, mu, siginv, beta, Ndoc) {
  expeta.sh <- exp(eta) 
  expeta <- c(expeta.sh,1)
  Ez <- expeta*beta
  denom <- doc.ct/.colSums(Ez, nrow(Ez), ncol(Ez))
  part1 <- (Ez%*%denom)[-length(expeta)] - expeta.sh*(Ndoc/sum(expeta))  
  part2 <- siginv%*%(eta-mu) 
  as.numeric(part2 - part1)
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
  part.rdd <- mapPartitionsWithIndex(documents.rdd, function(split,part) {
    beta.ss <- vector(mode="list", length=A)
    for(i in 1:A) {
      beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
    }
    sigma.ss <- diag(0, nrow=(K-1))
    lambda <- rep(NULL, times = K) # K - 1, plus 1 column for row order so we can sort later
    
    mu.in <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    siginv.in <- value(siginv.broadcast)
    sigmaentropy.in <- value(sigmaentropy.broadcast)
    bound <- sapply(part, function(document) {
      eta <- document$l
      words <- document$d[1,]
      if (ncol(mu.in) > 1) {
        mu.i <- mu.in[,document$dn]
      } else {
        mu.i <- as.numeric(mu.in)
      }
      
      #Solve for Hessian/Phi/Bound returning the result
      doc.results <- hpb(document$l, doc.ct=document$d[2,], mu=mu.i,
                               siginv=siginv.in, beta=beta.in[[document$a]][,words,drop=FALSE], document$nd ,
                               sigmaentropy=sigmaentropy.in)
      
      beta.ss[[document$a]][,words] <<- doc.results$phis + beta.ss[[document$a]][,words]
      sigma.ss <<- sigma.ss + doc.results$eta$nu
      lambda <<- rbind(lambda, c(document$dn, document$l))
      c(document$dn, doc.results$bound)
    })
    index <- as.integer(split/sqrt(spark.partitions))
    list(list(key = index, list(
                s = sigma.ss, 
                     b = beta.ss, 
                     bd = t(bound),
                     l = lambda
    )))
  })
  if (FALSE) {
    print("count")
    toss <- SparkR::count(part.rdd)
    print("count")
  }
  # try to combine using an intermediate step
  if ("KEY" %in% reduction) { # turn this on when we understand it better
    part.rdd <- reduceByKey(part.rdd, function(x, y) {
      if (length(x) == 4 && length(y) == 4) {
        list(bd = rbind(x$bd, y$bd), 
             s = x$s + y$s, 
             b = merge.beta(x$b, y$b), 
             l = rbind(x$l, y$l)
        )
      } else { 
        stop(paste("bad key reduction match",
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
    part <- numPartitions(part.rdd)
    print(paste("partitions before combining ", part))
    part.rdd <- combineByKey(part.rdd, createCombiner = function(v) v, 
                             mergeValue = function(C, v) {
      print("merge value")
      list(
           bd = rbind(C$bd, v$bd), 
           s = C$s + v$s, 
           b = merge.beta(C$b, v$b), 
           l = rbind(C$l, v$l)
      )
    },
    mergeCombiners = function(C1, C2) {
      print("merge combiners")
      list(
           bd = rbind(C1$bd, C2$bd), 
           s = C1$s + C2$s, 
           b = merge.beta(C1$b, C2$b), 
           l = rbind(C1$l, C2$l)
      )
    },
    numPartitions = as.integer(sqrt(spark.partitions))
    )
    print("done combining")
    part <- numPartitions(part.rdd)
    print(paste("partitions after combining ", part))
  }
  if ("REPARTITION" %in% reduction) {
    print("repartitioning")
    part <- numPartitions(part.rdd) 
    print(paste("before repartitioning ", part))
    part.rdd <- partitionBy(part.rdd, numPartitions = as.integer(sqrt(part)))
    print("repartitioned, now collapsing")
    part.rdd <- mapPartitionsWithIndex(part.rdd, function (split, part) {
      out <- part[[1]][[2]]
      for (index in 2:length(part)) {
        current <- part[[index]][[2]]
        out <- list(bd = rbind(out$bd, curent$bd), 
                    s = out$s + current$s, 
                    b = merge.beta(out$b, current$b), 
                    l = rbind(out$l, current$l)
        )
      }
      list(list(split, out))
    })
    print("collapsed")
  }
  if ("COLLECT" %in% reduction) {
    print("Collecting")
    toss <- collect(part.rdd)
    print("Done collecting")
  }
  if ("COLLECTPARTITION" %in% reduction) {
    print("collecting partitions - counting")
    j <- numPartitions(part.rdd)
    print("counted")
    for (i in 0:(j-1)) {
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
    if (length(x) == 2) x <- x[[2]]
    if (length(y) == 2) y <- y[[2]]
    if (length(x) == 4 && length(y) == 4) {
      list(bd = rbind(x$bd, y$bd), 
           s = x$s + y$s, 
           b = merge.beta(x$b, y$b), 
           l = rbind(x$l, y$l)
      )
    } else { 
      stop(paste("bad reduction match",
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
  counts.list <- lapply(counts, function(x) {
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
  if(doDebug) print("mnreg")
  mnreg.rdd <- mapPartitionsWithIndex(counts.rdd, function(split, part) {
    # each part should be a column from counts, representing a single vocabulary term
    if (doDebug) print("in mnreg")
    offset.in <- value(offset.broadcast)
    covar.in <- value(covar.broadcast)
    i.start <- part[[1]]$t
    map.out <- sapply(part, USE.NAMES=FALSE,simplify=FALSE,function(a.count) {
      print("in sapply")
      i <- a.count$t
      counts.i <- a.count$c.i
      m.i <- a.count$m.i
      if (is.null(m.i)) {
        offset2 <- offset.in
      } else {
        offset2 <- m.i + offset.in
      }
      
      mod <- NULL
      while(is.null(mod)) {
        mod <- tryCatch(glmnet(x=covar.in, y=counts.i, family="poisson", 
                               offset=offset2, standardize=FALSE,
                               intercept=is.null(m.i), 
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
      if(is.null(m.i)) coef <- c(mod$a0[lambda], coef)
#      c(i, coef)
      if (is.null(m.i)) {
        m.i <- coef[1]
        coef <- coef[-1]
      }
      coef <- covar.in[i,] %*% coef
      coef <- coef + m.i
      coef <- exp(coef)
      # if m.i is not NULL
        # then m is the first element, which gets removed
      # covar.in[i,] %*% coef
      # add m to every cell
      # exp()
      print(str(coef))
      coef #should we include some kind of index here?
    } )
    print("out of sapply")
    # now need to split it up into rows, and produce output that is keyed on row but identifies the columns
    print(str(map.out))
    out <- split(map.out, rep(1:nrow(map.out), ncol(map.out)))
    print(str(out))
    index <- 0
    lapply(out, function(x) {
      index <<- index + 1
      list(index, 
           list(i.start, #this won't work
             x
           ))
    })
  }) # output should be chunks of rows, keyed on row, identifying the starting column
  mnred.rdd <- combineByKey(mnreg.rdd, createCombiner = function(v) {
    C <- rep(NULL, V) #are we sure this is V?
    C[v[[1]]:(v[[1]] + length(v[[2]]))] <- v[[2]]
    C
  }, mergeValue = function(C, v) {
    C[v[[1]]:(v[[1]] + length(v[[2]]))] <- v[[2]]
    C
  }, mergeCombiners = `+`, as.integer(spark.partitions)) # output should be complete rows
  mnreg.rdd <- flatMap(mnreg.rdd, function(x) {
    row <- x[[1]]
    value <- x[[2]]/sum(x[[2]])
    out <- split(value, 1:length(value))
    index <- 0
    lapply(out, function(y) {
      index <<- index + 1
      position <- (index * K) + row # does this thing have K rows or V rows?
      list(floor(position / K), #aspect
           list(position %% K, y))
    })
  }) # output should be individual cells, keyed on aspect, and identifying position within 
  mnreg.rdd <- combineByKey(mnreg.rdd, createCombiner = function(v) {
    rows <- K # so its easy to swap if we get it wrong
    cols <- V
    C <- matrix(rep(NULL, V * K), nrow = rows) 
    C[v[[1]] %% rows, floor(v[[1]] / cols)] <- v[[2]]
    C
  }, mergeValue = function(C, v) {
    rows <- V # so its easy to swap if we get it wrong
    cols <- K
    C[v[[1]] %% rows, floor(v[[1]] / cols)] <- v[[2]]
    C
  }, mergeCombiners = `+`, as.integer(spark.partitions)) # output should be an rdd of the new beta
  beta <- collect(mnreg.rdd)
  beta.distributed <- distribute.beta(spark.context = spark.context, beta, spark.partitions)

  coef <- reduce(mnreg.rdd, function(x,y)  {   
    if ((is.null(x) || is.integer(x)) && !is.null(y)) return(y)
    if ((is.null(y) || is.integer(y)) && !is.null(x)) return(x)
    cbind(x, y)
  })

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
  
  linpred <- sweep(linpred, 2, STATS=m, FUN="+") #adds m to each row, by rows
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

envirolist <- function() {
  environments <- search()
  print(environments)
  environments <- environments[!grep("(package|autoloads)", environments)]
  for (e in environments) {
    print(e)
    print(ls(envir = as.environment(e)))
    for (item in ls(envir = as.environment(e))) {
      print(item)
      print(pryr::object_size(item))
    }
  }
  for (l in 1:(sys.nframe()-1)) {
    e <- ls(envir = parent.frame(l))
    print(e)
    for (i in e) {
      print(i)
      print(pryr::object_size(i))
    }
  }
}

# Hessian/Phi/Bound
#   NB: Hessian function is not as carefully benchmarked as it isn't called
#       nearly as often.  Particularly I suspect some of the elements in the
#       cross product could be sped up quite a bit.
#   NB: Bound and hessian barely communicate with one another here.
hpb <- function(eta, doc.ct, mu, siginv, beta, Ndoc=sum(doc.ct), sigmaentropy) {
  #basic transforms
  expeta <- c(exp(eta),1)
  theta <- expeta/sum(expeta)
  
  #pieces for the derivatives of the exp(eta)beta part
  EB <- expeta*beta #calculate exp(eta)\beta for each word
  EB <- t(EB)/colSums(EB) #transpose and norm by (now) the row
  
  #at this point EB is the phi matrix
  phi <- EB*(doc.ct) #multiply through by word count
  phisums <- colSums(phi)
  phi <- t(phi) #transpose so its in the K by W format expected
  EB <- EB*sqrt(doc.ct) #set up matrix to take the cross product
  
  #First piece is the quotient rule portion that shows up from E[z], second piece is the part
  # that shows up regardless as in Wang and Blei (2013) for example.  Last element is just siginv
  hess <- -((diag(phisums) - crossprod(EB)) - 
              Ndoc*(diag(theta) - theta%o%theta))[1:length(eta),1:length(eta)] + siginv
  
  ###
  # Bound
  
  nu <- try(chol2inv(chol.default(hess)), silent=TRUE)
  if(class(nu)=="try-error") {
    #brute force solve
    nu <- solve(hess)
    #only if we would produce negative variances do we bother doing nearPD
    if(any(diag(nu)<0)) nu <- as.matrix(nearPD(nu)$mat)
  }
  diff <- eta - mu
  logphinorm <- log(colSums(theta*beta))
  part1 <- sum(doc.ct*logphinorm)
  bound <- part1 + .5*determinant(nu, logarithm=TRUE)$modulus -
    .5*sum(diff*crossprod(diff,siginv)) -
    sigmaentropy
  bound <- as.numeric(bound)
  
  #bundle everything up.
  return(list(phis=phi, eta=list(lambda=eta, nu=nu), bound=bound))
}

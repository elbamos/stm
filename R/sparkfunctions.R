doDebug <- FALSE


estep.lambda <- function( 
  documents.rdd,
  beta.distributed,
  mu.distributed, 
  siginv.broadcast,
  spark.context,
  spark.partitions,
  verbose) {
  
  if (doDebug) print("Entering e-step")
  
  # perform logistic normal
  if (doDebug) print("mapping e-step lambda")
  mapPartitionsWithIndex(documents.rdd, function(split, part) {   
    mu <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    siginv <- value(siginv.broadcast)
#    print(paste("Entering logist normal 1", length(part)))
    out <- llply(.data = part, .fun = function(listElement) {
      if (doDebug) print(paste("logistic normal 1 start", class(listElement)))
      if (doDebug && is.null(listElement[[1]])) {
        print("logistic normal 1 list element is null")
        print(str(listElement))
        print(str(part))
      }
      if (doDebug && listElement[[1]] == 1) {
        print("logistic normal 1")
        print(str(listElement))
      }
      document <- listElement

      init <- document$l
      words <- document$d[1,]
      beta.i.lambda <- beta.in[[document$a]][,words,drop=FALSE]
      if (ncol(mu) > 1) {
        mu.i <- mu[,document$dn]
      } else {
        mu.i <- as.numeric(mu)
      }
        
        document$lambda <- logisticnormal.lambda(eta = init, 
                                            mu = mu.i, 
                                            siginv = siginv,
                                            beta = beta.i.lambda, 
                                            doc = document$d
        )
        document
      }
    )
#    print(paste("logistic partition output", object_size(out)))
    out
  })
}

logisticnormal.lambda <- function(eta, mu, siginv, beta, doc) {
  doc.ct <- doc[2,]
  Ndoc <- sum(doc.ct)
  #even at K=100, BFGS is faster than L-BFGS
  optim.out <- optim(par=eta, fn=stm:::lhood, gr=stm:::grad,
                     method="BFGS", control=list(maxit=500),
                     doc.ct=doc.ct, mu=mu,
                     siginv=siginv, beta=beta, Ndoc=Ndoc)
  optim.out$par
}

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

  
  # perform logistic normal
  if (doDebug) print("mapping e-step")
#  print(count(documents.rdd))
  part.rdd <- mapPartitionsWithIndex(documents.rdd, function(split, part) {
    beta.ss <- vector(mode="list", length=A)
    for(i in 1:A) {
      beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
    }
    sigma.ss <- diag(0, nrow=(K-1))
    
    lambda <- rep(NULL, times = K - 1)
    
    mu <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    #    lambda.in <- value(lambda.distributed)
    siginv <- value(siginv.broadcast)
    sigmaentropy <- value(sigmaentropy.broadcast)
    if (doDebug) {
      s <- sum(is.null(part)) 
      print(paste("In hpb map, ", s, " elements of the list are null."))
    }
    
    bound <- laply(part, .fun = function(listElement) {
      if (is.null(listElement)) next
      if (doDebug && listElement[[1]] == 1) print(str(listElement))
      document <- listElement
      if (doDebug && document$doc.num == 1) print(str(document))
      eta <- document$l
      words <- document$d[1,]
      if (ncol(mu) > 1) {
        mu.i <- mu[,document$dn]
      } else {
        mu.i <- as.numeric(mu)
      }
      
      doc.ct <- document$d[2,]
      Ndoc <- sum(doc.ct)
      #Solve for Hessian/Phi/Bound returning the result
      doc.results <- stm:::hpb(eta, doc.ct=doc.ct, mu=mu.i,
          siginv=siginv, beta=beta.in[[document$a]][,words,drop=FALSE], Ndoc=Ndoc,
          sigmaentropy=sigmaentropy)
      

      beta.ss[[document$a]][,words] <<- doc.results$phis + beta.ss[[document$a]][,words]
      sigma.ss <<- sigma.ss + doc.results$eta$nu
      lambda <<- rbind(lambda, document$l)
      c(document$dn, doc.results$bound)
#      if (is.null(bound)) {bound <- bd} else {bound <<- rbind(bound, bd)}
    })
#    lambda <- lambda[-1,]
    list(split, list(s = sigma.ss, 
                   b = beta.ss, 
                   bd = bound,
                   l = lambda
                   ))
  })

  reduce(part.rdd, op = function(x, y) {
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
}

merge.beta <- function(x, y) {
  for (i in 1:length(x)) x[[i]] <- x[[i]] + y[[i]]
  x
}

distribute.beta <- function(beta, spark.context, spark.partitions) {
    index <- 0
    if (doDebug) {
      print("beta")
      print(object_size(beta))
    }
   broadcast(sc = spark.context, beta)
}

distribute.mu <- function(mu, spark.context, spark.partitions) {
  if (doDebug) {
    print("mu")
    print(object_size(mu$mu))
  }
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
  if (! "list" %in% class(beta.ss)) print(str(beta.ss))
  if (doDebug) print(paste("mnreg beta ss", str(beta.ss)))
#  print(str(beta.ss))
  counts <- do.call(rbind,beta.ss)

if (doDebug) print(str(counts))

#
# Testing showed that covar was a constant -- if this is incorrect please let me know.
#
covar.broadcast <- settings$covar.broadcast
  
  if(fixedintercept) {  
    m <- settings$dim$wcounts$x
    m <- log(m) - log(sum(m))
  } else {
    m <- NULL #have to assign this to null to keep code simpler below
  }
#  m.broadcast <- broadcast(spark.context, m)
  mult.nobs <- rowSums(counts) #number of multinomial draws in the sample
  offset <- log(mult.nobs)
  offset.broadcast <- broadcast(spark.context, offset)

  counts <- split(counts, col(counts)) # now a list, indexed by term, of arrays

  index <- 0
if (doDebug) print("distributing counts")
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
  
  #now do some setup of infrastructure
  verbose <- settings$verbose

  #now iterate over the vocabulary
if (doDebug) print("Big map")
  mnreg.rdd <- mapPartitionsWithIndex(counts.rdd, function(split, part) {
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
if (doDebug)  print("going to reduce")

  coef <- reduce(mnreg.rdd, function(x,y)  {   
    if ((is.null(x) || is.integer(x)) && !is.null(y)) return(y)
    if ((is.null(y) || is.integer(y)) && !is.null(x)) return(x)
    rbind(x, y)
    }
  )
coef <- t(coef)
coef <- coef[,order(coef[1,])]
coef <- coef[-1,]


if (doDebug) print("reduced")

if (doDebug)  print("wrap up the function and redistribute beta")
  
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
  out <- list(beta = beta, kappa=kappa, nlambda=nlambda, beta.distributed = beta.distributed)
  return(out)
}

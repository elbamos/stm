# This implementation doesn't work right now.  The concept is to move some of the post-glmnet processing of
# opt beta out into the cluster.
mnreg.spark.distributedbeta <- function(beta.ss,settings, spark.context, spark.partitions) {
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
  
  if (!fixedintercept) stop("Must use fixed intercept for distributed opt beta.")
  
  counts <- do.call(rbind,beta.ss)
  
  covar.broadcast <- settings$covar.broadcast
  m.broadcast <- settings$m.broadcast
  
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
      c.i = x#, 
      #      m.i = ifelse(is.null(m), NULL, m[index])
    )
  })
  bf <- paste0(settings$betafile, round(rnorm(1) * 10000))
  saveAsObjectFile(parallelize(spark.context, counts.list, spark.partitions), bf)
  counts.rdd <- objectFile(spark.context, bf, spark.partitions)
  rm(counts.list)
  
  #########
  #Distributed Poissons
  #########

  verbose <- settings$verbose
  
  mnreg.rdd <- mapPartitionsWithIndex(counts.rdd, function(split, part) {
    # each part should be a column from counts, representing a single vocabulary term
    offset.in <- value(offset.broadcast)
    covar.in <- value(covar.broadcast)
    i.start <- part[[1]]$t
    m <- value(m.broadcast)
    
    coef <- sapply(part, USE.NAMES=FALSE,simplify=TRUE,function(a.count) {
      i <- a.count$t
      counts.i <- a.count$c.i
      offset2 <- m[i] + offset.in
      
      mod <- NULL
      while(is.null(mod)) {
        mod <- tryCatch(glmnet(x=covar.in, y=counts.i, family="poisson", 
                               offset=offset2, standardize=FALSE,
                               intercept=FALSE, 
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
      subM(mod$beta,lambda) #return coefficients
    } )

#    coef <- do.call(cbind,map.out)
    coef <- covar.in %*% coef # should have one column for each vocab in input, and rows = nrow(covar.in)
                              # there should be A*K rows
    if (nrow(coef) != A*K) stop("Coef AK Error")

    coef <- matrix(coef)

    coef <- sweep(coef, 2, 
                  STATS=m[i.start:(i.start + ncol(coef) - 1)], FUN="+")
    coef <- exp(coef)

    # want to split into one matrix per aspect.  First split into list of aspects, one vector each
    coef <- split(coef, rep(1:A, K)  )

    index <- 0
    lapply(coef, function(x) {
      index <<- index + 1
      list(aspect = index, 
           list(col = i.start, x = x))
    })
  })
  
  mnreg.rdd <- combineByKey(mnreg.rdd, createCombiner = function(v) {
    C <- matrix(rep(0, V * K), nrow = K)
    x <- matrix(v[[2]], nrow = K)
    C[,v[[1]]:(v[[1]] + ncol(x-1) - 1) ] <- x
    C
  }, mergeValue = function(C, v) {
    x <- matrix(v[[2]], nrow = K)
    C[,v[[1]]:(v[[1]] + ncol(x)-1) ] <- x
    C
  }, mergeCombiners = `+`, 
  as.integer(A)
  ) 
  
#    mnreg.rdd <- mapValues(mnreg.rdd, function(x) {
#      x <- x / rowSums(x)
#    })
  beta <- collectAsMap(mnreg.rdd)
  beta <- lapply(beta, function(x) { x / rowSums(x)})
#  beta <- lapply(beta, function(x) {x / rowSums(x)})
  # beta calculates, but beta is a key,value pair list, not just a list now
  beta.distributed <- distribute.beta(spark.context = spark.context, beta, spark.partitions)
  
  list(beta = beta, nlambda=nlambda, beta.distributed = beta.distributed)
}

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
  
  if (!fixedintercept) stop("Must use fixed intercept for distributed opt beta.")
  
  
  counts <- do.call(rbind,beta.ss)
  
  covar.broadcast <- settings$covar.broadcast
  m.broadcast <- settings$m.broadcast
  
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
      c.i = x
    )
  })
  bf <- paste0(settings$betafile, round(rnorm(1) * 10000))
  saveAsObjectFile(parallelize(spark.context, counts.list, spark.partitions), bf)
  counts.rdd <- objectFile(spark.context, bf, spark.partitions)
#  counts.rdd <- parallelize(spark.context, counts.list, spark.partitions)
  rm(counts.list)
  
  #########
  #Distributed Poissons
  #########

  
  verbose <- settings$verbose
  
  mnreg.rdd <- mapPartitionsWithIndex(counts.rdd, function(split, part) {
    # each part should be a column from counts, representing a single vocabulary term
    offset.in <- value(offset.broadcast)
    covar.in <- value(covar.broadcast)
    i.start <- part[[1]]$t
    m <- value(m.broadcast)
    
    map.out <- sapply(part, USE.NAMES=FALSE,simplify=TRUE,function(a.count) {
      i <- a.count$t
      counts.i <- a.count$c.i
      offset2 <- m[i] + offset.in
      
      mod <- NULL
      while(is.null(mod)) {
        mod <- tryCatch(glmnet(x=covar.in, y=counts.i, family="poisson", 
                               offset=offset2, standardize=FALSE,
                               intercept=FALSE, 
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
      c(i, coef)
    } )
    list(value = map.out)
  })
  
  
  coef <- reduce(mnreg.rdd, function(x,y)  {   
    if ((is.null(x) || is.integer(x)) && !is.null(y)) return(y)
    if ((is.null(y) || is.integer(y)) && !is.null(x)) return(x)
    cbind(x, y)
  })
  coef <- coef[,order(coef[1,])]
  coef <- coef[-1,]
  
  kappa <- split(coef, row(coef)) 
  ##
  #predictions 
  ##
  #linear predictor
  covar <- settings$covar
  
  linpred <- as.matrix(covar%*%coef) # preserves columns, but rows in output = rows in covar
  
  linpred <- sweep(linpred, 2, STATS=settings$m, FUN="+") # applies m against each row
  #softmax
  explinpred <- exp(linpred)
  
  beta <- explinpred/rowSums(explinpred)
  
  beta <- split(beta, rep(1:A, each=K))
  
  #wrangle into the list structure
  beta <- lapply(beta, matrix, nrow=K)
  beta.distributed <- distribute.beta(spark.context = spark.context, beta, spark.partitions)
  
  kappa <- list(params=kappa)
  list(beta = beta, kappa=kappa, nlambda=nlambda, beta.distributed = beta.distributed)
}

opt.mu.spark <- function(lambda, settings, mode=c("CTM","Pooled", "L1"), covar=NULL, enet=NULL) {
  mode <- settings$gamma$mode
  if (mode == "L1") return(stm:::opt.mu(lambda, mode, covar, enet))
  if (mode == "CTM") return(matrix(colMeans(lambda), ncol=1))
  index <- 0
  lambda.out <- apply(lambda, MARGIN=2, function(x) {
    index <<- index + 1
    list(index, x)
  })
  fn <- paste0(settings$mufile, round(rnorm(1) * 1000))
  saveObjectFile(parallelize(settings$spark.context, lambda.out, settings$spark.partitions), fn)
  lambda.rdd <- objectFile(spark.context, fn, settings$spark.partitions)
  covar <- settings$covar.broadcast
  
  mapPartitionsWithIndex(lambda.rdd, function(part) {
    covar.in <- value(covar)
    lapply(part, simplify = TRUE, FUN = function(a.lambda)
        list(a.lambda[[1]], 
             covar %*% vb.variational.reg(Y=a.lambda[[2]], X = covar)
        )
    ) 
  })
}

opt.sigma.spark <- function(sigma.ss, lambda, mu, settings) {  
  sigprior <- settings$sigma$prior
  # Assume that lambda is an rdd, and mu is an rdd unless its only one column
  onecol <- FALSE # is mu only 1 column?
  if (onecol) {
    covariance <- mapValues(lambda, function(x) {
      x - as.numeric(mu)
    })
  } else {
    covariance.rdd <- join(lambda, mu, spark.partitions = as.integer(settings$spark.partitions))
    covariance.rdd <- mapValues(covariance.rdd, function(x) {x[[1]] - x[[2]]})
  } 
  covar.cells.rdd <- mapPartitions(covariance.rdd, function(part) {
    startrow <- part[[1]][[1]]
    interim <- lapply(part, function(x) x[[2]])
    interim <- do.call(cbind, interim)
    index <- 0
    apply(interim, MARGIN=1, function(x) {
      index <<- index + 1
      list(index, 
           list(colstart = startrow,
                x
                ))
    })
  })
  covar.rows.rdd <- combineByKey(covar.cells.rdd, function(v) {
    C <- rep(0, K - 1)
    C[v[[1]]:(v[[1]] + length(v[[2]]) - 1)] <- v[[2]]
    C
  }, function(C, v) {
    C[v[[1]]:(v[[1]] + length(v[[2]]) - 1)] <- v[[2]]
    C
  }, 
  "+", 
  as.integer(spark.partitions)
  })
  crossprod.rdd <- fullOuterJoin(covar.rows.rdd, covariance.rdd)
  crossprod.rdd <- map
  
  # now need to take the crossproduct of covariance.rdd
  # then figure out how to do the below...
  sigma <- (covariance + nu)/nrow(lambda) #add to estimation variance
  sigma <- diag(diag(sigma),nrow=nrow(nu))*sigprior + (1-sigprior)*sigma #weight by the prior
  return(sigma)
}

#Variational Linear Regression with a Half-Cauchy hyperprior 
# (Implementation based off the various LMM examples from Matt Wand)
# This code is intended to be passed a Matrix object
vb.variational.reg <- function(Y,X, b0=1, d0=1) {
  Xcorr <- crossprod(X)
  XYcorr <- crossprod(X,Y) 
  
  an <- (1 + nrow(X))/2
  D <- ncol(X)
  N <- nrow(X)
  w <- rep(0, ncol(X))
  error.prec <- 1 #expectation of the error precision
  converge <- 1000
  cn <- ncol(X) # - 1 for the intercept and +1 in the update cancel
  dn <- 1
  Ea <- cn/dn #expectation of the precision on the weights
  ba <- 1
  
  while(converge>.0001) {
    w.old <- w
    
    #add the coefficient prior.  Form depends on whether X is a Matrix object or a regular matrix.
    if(is.matrix(X)) {
      ppmat <- diag(x=c(0, rep(as.numeric(Ea), (D-1))),nrow=D) 
    } else {
      ppmat <- Diagonal(n=D, x=c(0, rep(as.numeric(Ea), (D-1))))
    }
    invV <- error.prec*Xcorr + ppmat
    #if its a plain matrix its faster to use the cholesky, otherwise just use solve
    if(is.matrix(invV)) {
      V <- chol2inv(chol(invV))
    } else {
      #Matrix package makes this faster even when its non-sparse
      V <- solve(invV)     
    }
    w <- error.prec*V%*%XYcorr
    
    # parameters of noise model (an remains constant)
    sse <- sum((X %*% w - Y)^ 2)
    bn <- .5*(sse + sum(diag(Xcorr%*%V))) + ba
    error.prec <- an/bn
    ba <- 1/(error.prec + b0)
    
    #subtract off the intercept while working out the hyperparameters
    # for the coefficients
    w0 <- w[1]
    w <- w[-1]
    da <- 2/(Ea + d0)
    dn <- 2*da + (crossprod(w) + sum(diag(V)[-1]))
    Ea <- cn / dn
    #now combine the intercept back in 
    w <- c(w0,w)
    converge <- sum(abs(w-w.old))
  }
  return(w)
}





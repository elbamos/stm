# This implementation doesn't work right now.  The concept is to move some of the post-glmnet processing of
# opt beta out into the cluster.
mnreg.spark.distributedbeta <- function(hpb.rdd,br, settings, spark.context, spark.partitions) {
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
  
#  assert_that(!fixedintercerpt)
  
  covar.broadcast <- settings$covar.broadcast
  m.broadcast <- settings$m.broadcast
  
  mult.nobs <- br #number of multinomial draws in the sample
  offset <- log(mult.nobs)
  offset.broadcast <- broadcast(spark.context, offset)

  counts.rdd <- groupByKey(mapPartitions(hpb.rdd, function(part) {
      x <- Filter(function(f) f[[1]] == "b", part)
      x[[1]][[2]]
    }), as.integer(spark.partitions)
  )
  
  #########
  #Distributed Poissons
  #########
  
  verbose <- settings$verbose
  
  mnreg.rdd <- mapPartitions(counts.rdd, function(part) {
    # each part should be a column from counts, representing a single vocabulary term
    offset.in <- value(offset.broadcast)
    covar.in <- value(covar.broadcast)
    colidxs <- list()
    m <- value(m.broadcast)
    
    coef <- sapply(part, USE.NAMES=FALSE,simplify=TRUE,function(a.count) {
      i <- a.count[[1]]
      colidxs <<- c(colidxs, i)
      counts.i <- Reduce("+", a.count[[2]], rep(0, A*K))

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

    coef <- covar.in %*% coef # should have one column for each vocab in input, and rows = nrow(covar.in)
                              
    assert_that(nrow(coef) == A*K) # there should be A*K rows

    colidxs <- unlist(colidxs)    
    coef <- sweep(coef, 2, 
                  STATS=m[colidxs], FUN="+")
    coef <- exp(coef)
    # want to split into one matrix per aspect. 
    coef <- split(coef, rep(1:A, each = K)  )

    index <- 0
    lapply(coef, function(x) {
      index <<- as.integer(index + 1)
      list(aspect = index, 
           list(col = colidxs, x = matrix(x, nrow = K))
      )
    })
  })
  
  mnreg.rdd <- groupByKey(mnreg.rdd, as.integer(min(A, spark.partitions)))
  mnreg.rdd <- mapPartitions(mnreg.rdd, function(part) {
    lapply(part, function(x) {
      aspect <- x[[1]]
      C <- Reduce(x = x[[2]], f = function(y, z) 
        list(c(y[[1]], z[[1]]), cbind(y[[2]], z[[2]])))
      C <- C[[2]][,order(C[[1]])]
      list(aspect, C/rowSums(C))
    })
  })
  
  # We're collecting this only so we can broadcast it again because we haven't figured out a memory-efficient
  # way to join beta.rdd and documents.rdd.  
  beta <- collectAsMap(mnreg.rdd)
  beta <- beta[order(names(beta))]
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


opt.mu.spark <- function(hpb.rdd, mode=c("CTM","Pooled", "L1"), settings) {
  assert_that(mode == "Pooled")
#   if (mode == "L1") return(stm:::opt.mu(lambda, mode, covar, enet))
#   if (mode == "CTM") return(matrix(colMeans(lambda), ncol=1))
  N <- settings$dim$N
  K <- settings$dim$K

  covar <- settings$X.broadcast

  # extract the chunks of lambda columns from the hpb output
  lambda.rdd <- groupByKey(mapPartitions(hpb.rdd, function(part) {
    x <- Filter(function(f) f[[1]] == "l", part)
    x[[1]][[2]]
    }), as.integer(settings$spark.partitions)
  )
  
  covar.in <- value(covar)
  covar.in <- Matrix::as.matrix(covar.in)
  xcorr <- crossprod(covar.in)
  # consolidate columns, perform vb.variational.reg on each, multiply by covar to produce some rows of mu, and 
  # then split them up into chunks of the columns of completed mu
  mapPartitions(lambda.rdd, function(part) {
    colidxs <- list() #Reduce(x = part, function(x, y) c(x[[1]], y[[1]]))
    mumap <- sapply(part, USE.NAMES=FALSE, FUN=function(a.lambda) {
      colidxs <<- c(colidxs, a.lambda[[1]]) # columns of lambda used in output, rows of mu in output
      column <- vectorcombiner(a.lambda[[2]])
      covar.in %*% vb.variational.reg(Y=column, X = covar.in, Xcorr = xcorr)
    })

    index <- 0
    colidxs <- unlist(colidxs)
    apply(mumap, MARGIN=1, function(x) {
      index <<- index + 1
      list(as.integer(index), # column of mu, equivalent to doc number
           list(
             colidxs, # positions within the dn-specific mu vector
             x
           )
      )
    })
  })
}


opt.sigma.spark <- function(nu, documents.rdd, mu.rdd, settings) {  
  sigprior <- settings$sigma$prior

  old <- documents.rdd
  documents.rdd <- cogroup(documents.rdd, mu.rdd, numPartitions=as.integer(settings$spark.partitions))
  persist(documents.rdd, settings$spark.persistence)
  covariance.rdd <- mapPartitions(documents.rdd, function(part) {
    lapply(part, function(x) {
      list(x[[1]],
        x[[2]][[1]][[1]]$l - vectorcombiner(x[[2]][[2]])
      )
    })
  })
  covariance <- collectAsMap(covariance.rdd)
  covariance <- do.call(rbind,covariance[order(names(covariance))]) 
  covariance <- crossprod(covariance)
  unpersist(old)
  unpersist(mu.rdd)

  sigma <- (covariance + nu)/settings$dim$N #add to estimation variance
  sigma <- diag(diag(sigma),nrow=nrow(nu))*sigprior + (1-sigprior)*sigma #weight by the prior
  list(sigma, documents.rdd)
}


#Variational Linear Regression with a Half-Cauchy hyperprior 

vb.variational.reg <- function(Y,X,Xcorr, b0=1, d0=1) {
#  Xcorr <- crossprod(X)
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






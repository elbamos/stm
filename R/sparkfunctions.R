doDebug <- FALSE


estep.spark.better <- function( 
  N,
  V, 
  documents.rdd,
  beta.distributed,
  lambda.rdd, 
  mu.distributed, 
  sigma, 
  spark.context,
  spark.partitions,
  verbose) {
  
  print("Entering e-step")
  
  # 2) Precalculate common components
  sigmaentropy <- (.5*determinant(sigma, logarithm=TRUE)$modulus[1])
  siginv <- solve(sigma)
  
  #broadcast what we need
  if (doDebug) {
    print("sigma")
    print(object_size(sigma))
    print("siginv")
    print(object_size(siginv))
  }
  siginv.broadcast <- broadcast(spark.context, siginv)
  if (doDebug) print("broadcast siginv")
  sigmaentropy.broadcast <- broadcast(spark.context, sigmaentropy)
  if (doDebug) print("broadcast sigma")
  
#   # setup mu.i
#   if (! "Broadcast" %in% class(mu)) {
#     if (doDebug) print("cogrouping mu")
#     estep.rdd <- cogroup(documents.rdd, 
#                          mu,
#                          numPartitions = spark.partitions)
#   } else {
#     mu.carry <- mu
#     estep.rdd <- documents.rdd
#   }
  estep.rdd <- leftOuterJoin(documents.rdd, beta.distributed, spark.partitions)

  # perform logistic normal
  if (doDebug) print("mapping e-step")
  mapValues(estep.rdd, function(y) {
    if (doDebug) print("inside mapping e-step testing for mu")
    if (doDebug) {
      print(str(y))
    }
    document <- y[[1]]
    beta.i <- y[[2]]
    mu <- value(mu.distributed)
    print(str(mu))
    if (ncol(mu) > 1) mu <- mu[,i]
    mu <- as.numeric(mu)
#     if (length(y) <= 2) {
#       document <- y[[1]][[1]]
#       document$mu.i <- y[[2]][[1]]
#     } else {
#       document = y
#       document$mu.i <- as.numeric(value(mu.carry))
#     }
#     beta.i <- value(beta.distributed)[[document$aspect]]
    
#    doc <- document$document
#     if (!is.numeric(document$mu.i)) {
#       print("mu.i")
#       print(mu.i)
#       print(str(x))
#     }
    words <- document$document[1,]

    beta.i <- beta.i[,words,drop=FALSE]
    siginv <- value(siginv.broadcast)
    sigmaentropy <- value(sigmaentropy.broadcast)

    doc.results <- stm:::logisticnormal(eta = document$lambda, 
                                        mu = document$mu.i, 
                                        siginv = siginv,
                                        beta = beta.i, 
                                        doc = document$document,
                                        sigmaentropy  = sigmaentropy
    )
    if (doDebug) print("finished logistic normal")
    document$lambda <- doc.results$eta$lambda
    document$sigma <- doc.results$eta$nu
#    document$phis <- doc.results$phis
    beta.slice <- matrix(FALSE, ncol = V, nrow = nrow(doc.results$phis))
    beta.slice[,words] <- beta.slice[,words] + doc.results$phis
    document$beta.slice <- beta.slice
    if (doDebug) {
#      document$betachecksum <- sum(beta.slice) 
      document$phi.checksum <- sum(doc.results$phis)
    }
    document$bound <- doc.results$bound
    if (doDebug) print("finished big map")
    document
  }
  )
}

reduce.beta.nokappa <- function(x) {
  x/rowSums(x)
}

distribute.beta <- function(beta, spark.context, spark.partitions) {
    index <- 0
    if (doDebug) {
      print("beta")
      print(object_size(beta))
    }
#    broadcast(sc = spark.context, beta)
      beta <- llply(beta, function(x) {
        index <<- index + 1
        list(key = index, 
             beta = x)
      })
    parallelize(spark.context, beta, length(beta))
}

distribute.mu <- function(mu, spark.context, spark.partitions) {
  if (doDebug) {
    print("mu")
    print(object_size(mu$mu))
  }
#  if (is.null(mu$gamma)) {
#    mu <- mu$mu
    mu <- mu$mu
    broadcast(spark.context, mu)#)
#   } else {
#     index <- 0
#     mulist <- alply(mu$mu, .margins = 2, .dims = TRUE, 
#                     .fun = function(x) {
#                       index <<- index + 1
#                       list(key = index,
#                            mu.i <- x)
#                     } )
#     return(parallelize(spark.context, mulist,spark.partitions))
#   }
}

covar.old <- NULL
mnreg.spark <- function(beta.combined.rdd,settings, spark.context, spark.partitions) {
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
#  counts <- do.call(rbind,beta.ss)
if (doDebug) print("getting counts")
counts <- reduce(beta.combined.rdd, function(x, y) {
  if (doDebug) print("getting a count")
  print(str(x))
  print(str(y))
  if ("list" %in% class(x)) x <- x[[2]]
  if ("list" %in% class(y)) y <- y[[2]]
  rbind(x, y)
}) # but is this the right order???
if (doDebug) print("beta.ss -- If the model completes but the output is funny, the sorting of this matrix is a suspect")

#Three Cases
if(A==1) { #Topic Model
  covar <- diag(1, nrow=K)
}
if(A!=1) { #Topic-Aspect Models
  #Topics
  veci <- 1:nrow(counts)
  vecj <- rep(1:K,A)
  #aspects
  veci <- c(veci,1:nrow(counts))
  vecj <- c(vecj,rep((K+1):(K+A), each=K))
  if(interact) {
    veci <- c(veci, 1:nrow(counts))
    vecj <- c(vecj, (K+A+1):(K+A+nrow(counts)))
  }
  vecv <- rep(1,length(veci))
  covar <- sparseMatrix(veci, vecj, x=vecv)
}  
if (doDebug) {
  print("covar comparison")
  if (! is.null(covar.old)) print(sum(covar - covar.old))
  covar.old <<- covar
}
covar.broadcast <- broadcast(spark.context, covar)
#counts <- rbind(beta.ss)

  
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
    list(term = index, 
         counts.i = x, 
         m.i = ifelse(is.null(m), NULL, m[index])
    )
  })
  counts.rdd <- parallelize(spark.context, counts.list, min(length(counts.list), spark.partitions))
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
#  ctevery <- ifelse(transposed.length>100, floor(transposed.length/100), 1)
#  out <- vector(mode="list", length=transposed.length)
  #now iterate over the vocabulary
if (doDebug) print("Big map")
  mnreg.rdd <- mapValues(counts.rdd, function(x) {
    i <- x$term
    counts.i <- x$couints.i
    m.i <- ifelse(is.null(x$m.i), 0, x$m.i)
    offset <- m.i + value(offset.broadcast)

    mod <- NULL
    while(is.null(mod)) {
      mod <- tryCatch(glmnet(x=value(covar.broadcast), y=counts.i, family="poisson", 
                             offset=offset, standardize=FALSE,
                             intercept=is.null(x$m.i), 
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
    if(is.null(x$m.i)) coef <- c(mod$a0[lambda], coef)

    coef
  })
if (doDebug)  print("going to reduce")
  out <- reduce(mnreg.rdd, cbind) 
if (doDebug) print("reduced")
  out <- out[,order(out[1,])]
  coef <- out[-1,]
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

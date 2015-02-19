doDebug <- FALSE


estep.spark.better <- function( 
  N,
  V, 
  documents.rdd,
  beta.rdd,
  lambda.rdd, 
  mu, 
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
  
  # setup mu.i
  if (! "Broadcast" %in% class(mu)) {
    if (doDebug) print("cogrouping mu")
    estep.rdd <- cogroup(documents.rdd, 
                         mu,
                         numPartitions = spark.partitions)
  } else {
    mu.carry <- mu
    estep.rdd <- documents.rdd
  }

  # Join with mu, if necessary, and set the key to 'aspect' so can be joined with beta
  estep.rdd <- map(estep.rdd, function(x) {
    if (length(x[[2]]) > 2) {
      list(aspect = x[[2]]$aspect, document = x[[2]])
    } else {
      doc <- x[[2]][[1]][[1]]
      doc$mu.i <- x[[2]][[2]][[1]]
      list(aspect = doc$aspect, 
           doc)
    }
    })
  if (doDebug) print ("join")
  estep.rdd <- join(estep.rdd, beta.rdd, numPartitions = spark.partitions)
  # perform logistic normal
  if (doDebug) print("mapping e-step")
  map(estep.rdd, function(y) {
      document = y[[2]][[1]]
      beta.i <- y[[2]][[2]]
      if(is.null(beta.i)) stop(paste("no beta", str(y)))
#      if ("Broadcast" %in% class(mu)) document$mu.i <- as.numeric(value(mu)) 
      if (! is.null(mu.carry)) document$mu.i <- as.numeric(value(mu.carry))
      # do we really need to braodcast this if its only one matrix?
      
    if (doDebug && document$doc.num == 1) {
      print(y)
      print(str(y))
      print("environment")
      print(ls.str())
    }
    doc <- document$document
    if (!is.numeric(document$mu.i)) {
      print("mu.i")
      print(mu.i)
      print(str(x))
    }
    words <- doc[1,]

    beta.i <- beta.i[,words,drop=FALSE]
    siginv <- value(siginv.broadcast)
    sigmaentropy <- value(sigmaentropy.broadcast)

    doc.results <- stm:::logisticnormal(eta = document$lambda, 
                                        mu = document$mu.i, 
                                        siginv = siginv,
                                        beta = beta.i, 
                                        doc = doc,
                                        sigmaentropy  = sigmaentropy
    )
    if (doDebug) print("finished logistic normal")
    document$lambda <- doc.results$eta.lambda
    document$sigma <- doc.results$eta.nu
#     doc.results$doc.num <- document$doc.num
#     doc.results$key <- document$key
#     doc.results$document <- doc
#     doc.results$aspect <- document$aspect
#     doc.results$lambda <- doc.results$eta$lambda

    beta.slice <- Matrix(FALSE, ncol = V, nrow = nrow(doc.results$phis))
    beta.slice[,words] <- beta.slice[,words] + doc.results$phis
    document$beta.slice <- beta.slice
#     doc.results$beta.slice <- beta.slice
    if (doDebug) {
      document$betachecksum <- sum(beta.slice) 
      document$phi.checksum <- sum(doc.results$phis)
    }
    document$bound.output <- c(document$doc.num, doc.results$bound)
if (doDebug) print("finished big map")
    list(key = doc.results$doc.num, document = document)
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
    betalist <- llply(beta, .fun = function(x) {
      index <<- index + 1
      list(key = index, 
           beta.slice = x)
    })
    parallelize(spark.context, betalist, spark.partitions)
}

distribute.mu <- function(mu, spark.context, spark.partitions) {
  if (doDebug) {
    print("mu")
    print(object_size(mu$mu))
  }
  if (is.null(mu$gamma)) {
    mu <- mu$mu
    return(broadcast(spark.context, mu))
  } else {
    index <- 0
    mulist <- alply(mu$mu, .margins = 2, .dims = TRUE, 
                    .fun = function(x) {
                      index <<- index + 1
                      list(key = index,
                           mu.i <- x)
                    } )
    return(parallelize(spark.context, mulist,spark.partitions))
  }
}


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
  if ("list" %in% class(x)) x <- x[[2]]
  if ("list" %in% class(y)) y <- y[[2]]
  rBind(x, y)
}) # but is this the right order???
if (doDebug) print("beta.ss -- If the model completes but the output is funny, the sorting of this matrix is a suspect")


#counts <- rbind(beta.ss)
  
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
  covar.broadcast <- broadcast(spark.context, covar)
  
  if(fixedintercept) {  
    m <- settings$dim$wcounts$x
    m <- log(m) - log(sum(m))
  } else {
    m <- NULL #have to assign this to null to keep code simpler below
  }
  
  mult.nobs <- rowSums(counts) #number of multinomial draws in the sample
  offset <- log(mult.nobs)

  counts <- split(counts, col(counts)) # now a list, indexed by term, of arrays
  index <- 0
if (doDebug) print("distributing counts")
  counts.list <- llply(counts, function(x) {
    index <<- index + 1
    list(term = index, value = x)
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
  mnreg.rdd <- map(counts.rdd, function(x) {
    i <- x[[1]]
    counts.i <- x[[2]]
    if(is.null(m)) {
      offset2  <- offset
    } else {
      offset2 <- m[i] + offset    
    }
    mod <- NULL
    while(is.null(mod)) {
      mod <- tryCatch(glmnet(x=value(covar.broadcast), y=counts.i, family="poisson", 
                             offset=offset2, standardize=FALSE,
                             intercept=is.null(m), 
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
    if(is.null(m)) coef <- c(mod$a0[lambda], coef)

    c(i,coef)
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
  beta <- lapply(beta, Matrix, nrow=K)
  beta.rdd <- distribute.beta(spark.context = spark.context, beta, spark.partitions)

  kappa <- list(m=m, params=kappa)
  out <- list(beta = beta, kappa=kappa, nlambda=nlambda, beta.rdd = beta.rdd)
  return(out)
}

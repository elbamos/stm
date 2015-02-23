doDebug <- FALSE


estep.spark.partition <- function( 
  N,
  V, 
  K,
  A,
  documents.rdd,
  beta.distributed,
  lambda.distributed, 
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
  
  combined.rdd <- cogroup(documents.rdd, lambda.distributed, numPartitions = spark.partitions)
  
  # perform logistic normal
  if (doDebug) print("mapping e-step")
  part.rdd <- mapPartitionsWithIndex(combined.rdd, function(split, part) {
    beta.ss <- vector(mode="list", length=A)
    for(i in 1:A) {
      beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
    }
    
    sigma.ss <- diag(0, nrow=(K-1))
    bound <- list()
    lambda <- list()
    
    mu <- value(mu.distributed)
    beta.in <- value(beta.distributed)
#    lambda.in <- value(lambda.distributed)
    siginv <- value(siginv.broadcast)
    sigmaentropy <- value(sigmaentropy.broadcast)
    for (combined in part) {
      if (doDebug) print(str(part))
      document <- part[[1]]
      init <- part[[2]]
      beta.i <- beta.in[[document$aspect]]
      if (ncol(mu) > 1) {
        mu.i <- mu[,document$doc.num]
      } else {
        mu.i <- as.numeric(mu)
      }
      
      words <- document$document[1,]
      beta.i <- beta.i[,words,drop=FALSE]
      
      doc.results <- stm:::logisticnormal(eta = init, 
                                          mu = mu.i, 
                                          siginv = siginv,
                                          beta = beta.i, 
                                          doc = document$document,
                                          sigmaentropy  = sigmaentropy
      )
      if (doDebug) print("finished logistic normal")
      beta.ss[[document$aspect]][,words] <- doc.results$phis + beta.ss[[document$aspect]][,words]
      if (doDebug)print("done beta")
      lambda[[document$doc.num]] <- c(document$doc.num, doc.results$eta$lambda)
      if (doDebug)print("done lambda")
      sigma.ss <- sigma.ss + doc.results$eta$nu
      if (doDebug)print("done sigma")
      bound[[document$doc.num]] <- c(document$doc.num, doc.results$bound)
    }
    if (doDebug)print("making a list")
    list(split,list(lambda = lambda, sigma.ss = sigma.ss, beta.ss = beta.ss, bound = bound))
  })
  reduce(part.rdd, function(x, y) {
    if (length(x) == 4 && length(y) == 4) {
    list(lambda = c(x$lambda, y$lambda), 
         bound = c(x$bound, y$bound), 
         sigma.ss = x$sigma.ss + y$sigma.ss, 
         beta.ss = merge.beta(x$beta.ss, y$beta.ss))
    } else { 
    y
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
#       beta <- llply(beta, function(x) {
#         index <<- index + 1
#         list(key = index, 
#              beta = x)
#       })
#     parallelize(spark.context, beta, length(beta))
}

distribute.lambda <- function(lambda, spark.context, spark.partitions) {
  index <- 0
  lambdalist <- alply(lambda, .margins=1, .fun = function(x) {
    index <<- index + 1
    #    list(key = betaindex[index], 
    list(key = index, 
         lambda = x)
  })
  #       beta <- llply(beta, function(x) {
  #         index <<- index + 1
  #         list(key = index, 
  #              beta = x)
  #       })
  parallelize(spark.context, lambdalist, spark.partitions)
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
           term = index, 
          counts.i = x, 
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
#  ctevery <- ifelse(transposed.length>100, floor(transposed.length/100), 1)
#  out <- vector(mode="list", length=transposed.length)
  #now iterate over the vocabulary
if (doDebug) print("Big map")
  mnreg.rdd <- mapPartitionsWithIndex(counts.rdd, function(split, part) {
    offset.in <- value(offset.broadcast)
    covar <- value(covar.broadcast)
    out <- list()
    for (count in part) {
      if (doDebug) print(str(part))
      i <- count$term
      counts.i <- count$counts.i
      m.i <- ifelse(is.null(count$m.i), 0, count$m.i) + offset.in
      mod <- NULL
      while(is.null(mod)) {
        mod <- tryCatch(glmnet(x=covar, y=counts.i, family="poisson", 
                               offset=offset, standardize=FALSE,
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
    if(is.null(x$m.i)) coef <- c(mod$a0[lambda], coef)
    out[[i]] <- c(i, coef)
  } 
  out <- do.call(cbind, out)
  list (key = split, coef = out)
  }
  )
if (doDebug)  print("going to reduce")
coef <- reduce(mnreg.rdd, cbind) 
coef <- coef[,order(coef[1,])]
coef <- coef[-1,]

if (doDebug)  print(str(coef))
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
 linpred <- as.matrix(settings$covar%*%coef) 
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

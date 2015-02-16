
estep.spark.better <- function( 
  N,
  V, 
  documents.rdd,
  beta.rdd,
  lambda.rdd, 
  mu, 
  sigma, 
  spark.context,
  verbose) {
  
  ctevery <- ifelse(N>100, floor(N/100), 1)
  
  if (is.null(mu$gamma)) {
    mu <- mu$mu
    mu <- broadcast(spark.context, mu)
  } else {
    index <- 0
    mulist <- alply(mu$mu, .margins = 2, .dims = TRUE, 
                    .fun = function(x) {
                      index <<- index + 1
                      list(key = index,
                           mu.i <- x)
                    } )
    mu  <- parallelize(spark.context, mulist)
  }
  
  # 2) Precalculate common components
  sigmaentropy <- (.5*determinant(sigma, logarithm=TRUE)$modulus[1])
  siginv <- solve(sigma)
  
  #broadcast what we need
  sigmaentropy.broadcast <- broadcast(spark.context, sigmaentropy)
  siginv.broadcast <- broadcast(spark.context, siginv)
  
  # setup mu.i
  if (! "Broadcast" %in% class(mu)) {
    estep.rdd <- cogroup(documents.rdd, 
                         mu,
                         numPartitions = as.integer(N))
  } else {
    estep.rdd <- documents.rdd
  }
  # merge with the right slice of beta
  estep.rdd <- keyBy(estep.rdd, function(x) {x[[2]]$aspect})
  estep.rdd <- leftOuterJoin(estep.rdd, beta.rdd, numPartitions = N)
  # perform logistic normal
  map(estep.rdd, function(y) {
      if (is.null(y[[2]][[1]][[2]][["lambda"]])) {
        # This executes if mu.i is being updated
        print("need to figure out mu.i")
        print(str(y))
        y[[2]][[1]]$mu.i <- y[[2]][[2]]
        y[[2]] <- y[[2]][[1]]
      } else {
        y[[2]][[1]][[2]]$mu.i <- as.numeric(value(mu)) # do we really need to braodcast this if its only one matrix?
      }
      x <- list(key = y[[2]][[1]][[2]]$key, 
                  document = y[[2]][[1]][[2]])
      x$document$betaslice <- y[[2]][[2]]
      if(is.null(x$document$betaslice)) stop(paste("no beta", str(y)))

    if (x$key == 1) {
      print("y")
      print(str(y))
      print("x")
      print(str(x))
    }

    key = x$key
    init <- x[["document"]][["lambda"]] 
    doc <- x[["document"]][["document"]] 
    mu.i <- x[["document"]][["mu.i"]] 
    if (!is.numeric(mu.i)) {
      print("mu.i")
      print(mu.i)
      print(str(x))
    }
    words <- doc[1,]
    beta.i = x[["document"]][["betaslice"]]
    beta.i <- beta.i[,words,drop=FALSE]
    siginv <- value(siginv.broadcast)
    sigmaentropy <- value(sigmaentropy.broadcast)

    doc.results <- stm:::logisticnormal(eta = init, 
                                        mu = mu.i, 
                                        siginv = siginv,
                                        beta = beta.i, 
                                        doc = doc,
                                        sigmaentropy  = sigmaentropy
    )

    doc.results$doc.num <- x[["document"]][["doc.num"]]
    doc.results$key <- x[["document"]][["key"]]
    doc.results$document <- doc
    doc.results$aspect <- x[["document"]][["aspect"]]
    lambda <- list()
    lambda[[doc.results$doc.num]] <- doc.results$eta$lambda
    doc.results$lambda <- lambda
    doc.results$lambda.output <- c(doc.results$doc.num, doc.results$eta$lambda)
    beta.slice <- matrix(0, ncol = V, nrow = nrow(doc.results$phis))
    beta.slice[,words] <- beta.slice[,words] + doc.results$phis
    doc.results$beta.slice <- beta.slice
    doc.results$bound.output <- c(doc.results$doc.num, doc.results$bound)
    if (is.null(doc.results$iteration)) {doc.results$iteration <- 1} else {
      doc.results$iteration <- doc.results$iteration + 1
    }
    list(key = x[[1]], doc.results = doc.results)
  }
  )
}

estep.spark <- function( 
    documents.broadcast,
    documents,
    beta.index.broadcast,
    beta.index,
    update.mu, #null allows for intercept only model  
    beta, 
    lambda.old, 
    mu, 
    sigma, 
    sparkContext,
    verbose) {
  
  V <- ncol(beta[[1]])
  K <- nrow(beta[[1]])
  N <- length(documents)
  A <- length(beta)
  ctevery <- ifelse(N>100, floor(N/100), 1)
  
  if(!update.mu) {
    mu.i <- as.numeric(mu)
  } else {
    mu.i <- mu[,i]
  }
  
  # 1) Initialize Sufficient Statistics 
  sigma.ss <- diag(0, nrow=(K-1))
  beta.ss <- vector(mode="list", length=A)
  for(i in 1:A) {
    beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
  }
  bound <- vector(length=N)
  lambda <- vector("list", length=N)
  
  # 2) Precalculate common components
  sigmaentropy <- (.5*determinant(sigma, logarithm=TRUE)$modulus[1])
  siginv <- solve(sigma)
  
  #broadcast what we need
  sigmaentropy.broadcast <- broadcast(sparkContext, sigmaentropy)
  siginv.broadcast <- broadcast(sparkContext, siginv)
  
  # Make a list
  estep.rdd <- list()
  for (i in 1:N) { # This should be to N, and we should change it to use document names once that has been put in
    aspect <- beta.index[i]
    words <- documents[[i]][1,]
    docdata <- list(
      key = i,
      init = lambda.old[i,], 
      beta.i = beta[[aspect]][,words,drop=FALSE],
      mu.i =  mu.i# ifelse(update.mu, mu[,i], mu.i)
    )
    estep.rdd[[i]] <- docdata
  }
  estep.rdd <- parallelize(sparkContext, estep.rdd)
  # If I make documents an RDD, should I run cogroup here?
  resultRDD <- SparkR::lapply(estep.rdd, function(x) {
    i <- x[[1]]
    doc <- value(documents.broadcast)[[i]]
    #    doc <- lookup(documents.rdd, i)[[1]]
    siginv <- value(siginv.broadcast)
    sigmaentropy <- value(sigmaentropy.broadcast)
    init <- x[[2]]
    mu.i <- x[[4]]
    beta.i <- x[[3]]
    doc.results <- stm:::logisticnormal(eta = init, 
                                        mu = mu.i, 
                                        siginv = siginv,
                                        beta = beta.i, 
                                        doc = doc,
                                        sigmaentropy  = sigmaentropy
    )
    list(key = i, doc.results = doc.results)
  }
  )
  
  results <- collect(resultRDD, flatten = FALSE)[[1]]
  for (i in 1:N) { # this should be an lapply, but whatever
    key <- results[[i]]$key
    doc.results <- results[[i]]$doc.results
    words <- documents[[i]][1,]
    aspect <- beta.index[i]
    sigma.ss <- sigma.ss + doc.results$eta$nu
    aspect <- beta.index[i]
    beta.ss[[aspect]][,words] <- doc.results$phis + beta.ss[[aspect]][,words]
    bound[i] <- doc.results$bound
    lambda[[i]] <- doc.results$eta$lambda
    if(verbose && i%%ctevery==0) cat(".")
  }
  # Here is where to deal with any errors processing the results of the map
  
  
  if(verbose) cat("\n") #add a line break for the next message.
  
  #4) Combine and Return Sufficient Statistics
  lambda <- do.call(rbind, lambda)
  return(list(sigma=sigma.ss, beta=beta.ss, bound=bound, lambda=lambda))
}

reduce.beta.nokappa <- function(x) {
  print("beta no kappa reducer")
  print(str(x))
  x/rowSums(x)
}

distribute.beta <- function(beta, spark.context) {
    index <- 0
    betalist <- llply(beta, .fun = function(x) {
      index <<- index + 1
      list(key = index, 
           beta.slice = x)
    })
    parallelize(spark.context, betalist)
}

distribute.mu <- function(mu, spark.context, doc.keys) {
    if (is.null(mu$gamma)) {
      mu <- mu$mu
      return(broadcast(spark.context, mu))
    } else {
      index <- 0
      mulist <- alply(mu$mu, .margins = 2, .dims = TRUE, 
                      .fun = function(x) {
       index <<- index + 1
       list(key = doc.keys[index],
            mu.i <- x)
     } )
      return(parallelize(spark.context, mulist))
    }
}
distribute.lambda <- function(lambda, spark.context, doc.keys) {
  index <- 0
  lambdalist <- alply(lambda, .margins = 1, .dims = TRUE, 
                    .fun = function(x) {
                      index <<- index + 1
                      list(key = index, #doc.keys[index],
                           lambda <- x)
                    } )
    return(parallelize(spark.context, lambdalist))
}


mnreg.spark <- function(beta.combined.rdd,settings) {
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
#  New row count from beta...
   beta.row.count.rdd <- mapValues(beta.combined.rdd, nrow)
   beta.row.count <- reduce(beta.row.count.rdd, "+")

  
  #Three Cases
  if(A==1) { #Topic Model
    covar <- diag(1, nrow=K)
  }
  if(A!=1) { #Topic-Aspect Models
    #Topics
    veci <- 1:beta.row.count
    vecj <- rep(1:K,A)
    #aspects
    veci <- c(veci,1:beta.row.count)
    vecj <- c(vecj,rep((K+1):(K+A), each=K))
    if(interact) {
      veci <- c(veci, 1:beta.row.count)
      vecj <- c(vecj, (K+A+1):(K+A+beta.row.count))
    }
    vecv <- rep(1,length(veci))
    covar <- sparseMatrix(veci, vecj, x=vecv)
  }
  
  
  if(fixedintercept) {  
    m <- settings$dim$wcounts$x
    m <- log(m) - log(sum(m))
  } else {
    m <- NULL #have to assign this to null to keep code simpler below
  }
  
  # QUESTION: DO WE NEED TO SORT THIS IN ANY WAY?
  beta.row.sums.rdd <- mapValues(beta.combined.rdd, rowSums)
  mult.nobs <- reduce(beta.row.count.rdd, rbind)

#  mult.nobs <- rowSums(counts) #number of multinomial draws in the sample
  offset <- log(mult.nobs)

#  counts <- split(counts, col(counts))
  col.i <- 1
  beta.transpose.rdd <- map(beta.combined.rdd, function(x) {
    list(#aspect = x[[1]], 
         column = 1, 
         value = cbind(x[[1]], 1:nrow(x[[2]]), x[[2]][,1])
    )
  })
  while (col.i < V) {
    col.i <- col.i + 1
    beta.transpose.rdd <- unionRDD(beta.transpose.rdd, map(beta.combined.rdd, function(x) {
      list(#aspect = x[[1]], 
           column = col.i, 
           value = cbind(x[[1]], 1:nrow(x[[2]]), x[[2]][,col.i])
           )
    }))
  }
  beta.transpose.rdd <- combineByKey(beta.transpose.rdd, 
                                      createCombiner = function(v) {v},
                                      mergeValue = rbind, 
                                      mergeCombiners = rbind,
                                      numPartitions = V)
  beta.transpose.rdd <- mapValues(beta.recombined.rdd, function(x) {x[order(x[,1], x[,2]),c(-1,-2)]})
#  transposed.length <- count(beta.transpose.rdd)
  
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
  mnreg.rdd <- map(beta.transposed.rdd, function(x) {
    i <- x[[1]]
    counts.i <- x[[2]]
    if(is.null(m)) {
      offset2  <- offset
    } else {
      offset2 <- m[i] + offset    
    }
    mod <- NULL
    while(is.null(mod)) {
      mod <- tryCatch(glmnet(x=covar, y=counts.i, family="poisson", 
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
    #    out[[i]] <- coef
    # Assuming that coef is a vector...
    c(i, coef) 
  })
  # for now, we have to recover coef
  coef <- reduce(mnreg.rdd, func = cbind)
  coef <- coef[,order(coef[1,])]
  coef <- coef[-1,]

#  coef <- do.call(cbind, out)
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
  
  #wrangle into the list structure
  beta <- split(beta, rep(1:A, each=K))
  beta <- lapply(beta, matrix, nrow=K)
  
  kappa <- list(m=m, params=kappa)
  out <- list(beta=beta, kappa=kappa, nlambda=nlambda)
  return(out)
}

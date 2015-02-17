
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
      if (! "Broadcast" %in% class(mu)) {
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

    key = x$key
    init <- x[["document"]][["lambda"]]
    if ("list" %in% class(init)) init <- init[[x[["document"]]$doc.num]]
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


mnreg.spark <- function(beta.combined.rdd,settings, spark.context) {
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
   print("Counting rows")
#   
    cache(beta.combined.rdd)
#    beta.row.count.rdd <- map(beta.combined.rdd, function(x) {
#      nrow(x[[2]])
#      })
#    beta.row.count <- reduce(beta.row.count.rdd, "+")
beta.row.count <- A * K
  
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

# 
covar.broadcast <- broadcast(spark.context, covar)
  
  
  if(fixedintercept) {  
    m <- settings$dim$wcounts$x
    m <- log(m) - log(sum(m))
  } else {
    m <- NULL #have to assign this to null to keep code simpler below
  }
  
  # QUESTION: DO WE NEED TO SORT THIS IN ANY WAY?
  print("Getting row sums")
  #NOTE - IT IS SUSPICIOUS WHETHER THIS WORKS.  IT WAS RETURNING A K * A MATRIX, WHEN I'M EXPECTING A SINGLE 
  # VECTOR OF K * A LENGTH.  
  beta.row.sums.rdd <- map(beta.combined.rdd, function(x) {
    list(1,list(x[[1]], log(rowSums(x[[2]]))))
    })
  print("Mult.nobs")
  mult.nobs <- combineByKey(beta.row.sums.rdd, createCombiner = function(v) {
                            ret <- rep(0, length = A * K) 
                            start <- 1 +((v[[1]] - 1) * K)
                            end <- start + K - 1
                            ret[start:end] <- ret[start:end] + v[[2]]
                            ret},
                            function(C, v) {
                              start <- 1 + ((v[[1]] - 1) * K)
                              end <- start + K - 1
                              C[start:end] <- C[start:end] + v[[2]]                              
                              C
                            },
                            "+",
                            1L)
  offset <- take(mult.nobs, 1L)[[1]][[2]]
#  mult.nobs <- rowSums(counts) #number of multinomial draws in the sample
#  offset <- log(mult.nobs)

#  counts <- split(counts, col(counts))

  # beta.combined.rdd should be a list of matrices beta[[aspect]][matrix]
  print("transpose - flatten")
 beta.transpose.rdd <- flatMap(beta.combined.rdd, function(x) {
# Produce an rdd of key value pairs of the form:  list(column index, list(aspect, column contents))
    col.i <- 0
    llply(split(x[[2]], col(x[[2]])), .fun = function(y) {
      col.i <<- col.i + 1
      list(column = col.i, list(x[[1]], y))
    })
})
  print("beta transpose combine")
  unpersist(beta.combined.rdd)
  beta.transpose.rdd <- combineByKey(beta.transpose.rdd, createCombiner = function(v) {
    ret <- matrix(0, nrow = A * K, ncol = 1)
      start <- 1 + ((v[[1]] - 1) * K)
      end <- start + K - 1
      ret[start:end] <- ret[start:end] + v[[2]]
      ret},
    mergeValue = function(C, v) {
      start <- 1 + ((v[[1]] - 1) * K)
      end <- start + K - 1
      C[start:end] <- C[start:end] + v[[2]]
      C
    }  , 
    # This should finish the transpose.  The rdd should now have one entry per column, keyed by column id, where
    # the value is a column of the merged columns from each aspect matrix.
    mergeCombiners = "+",
    1L)
  # For debugging purposes
  cache(beta.transpose.rdd)
  bob <- take(beta.transpose.rdd, 1L)
  print("beta transpose.rdd debug output")
  print(str(bob))
  
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
  print("Big map")
  mnreg.rdd <- map(beta.transpose.rdd, function(x) {
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
    #    out[[i]] <- coef

    list(i, coef) 
  })
  unpersist(beta.transpose.rdd)
  cache(mnreg.rdd)
  # for debugging purposes
  bob <- take(mnreg.rdd, 1L)
  print("big map debug")
  print(str(bob))
  # for now, we have to recover coef
#   coef <- reduce(mnreg.rdd, func = cbind)
#   coef <- coef[,order(coef[1,])]
#   coef <- coef[-1,]

#  coef <- do.call(cbind, out)
  if(!fixedintercept) {
    #if we estimated the intercept add it in
    m <- lookup(mnreg.rdd, 1)[[1]] 
  }
  #kappa <- split(coef, row(coef)) # NEED TO DEAL WITH THIS -- WILL FAIL RIGHT NOW
  ##
  #predictions 
  ##
  #linear predictor
#   linpred <- as.matrix(covar%*%coef) #covar needs to be distributed
# linpred <- sweep(linpred, 2, STATS=m, FUN="+")
#softmax
# explinpred <- exp(linpred)
  linpred.rdd <- flatMap(mnreg.rdd, function(x) {
    # The key is the id of a column of coef.  We are multiplying each row of covar by each column of coef.
    covar <- value(covar.broadcast)
    index <- 0
    alply(covar, .margins = 1, .fun = function(x) {
      index <<- index + 1 
      list(
        row <- index,
        list(
          column <- x[[1]], 
#           linpred = (x * covar[index,]) + m[index], 
           explinpred = exp((x[[2]] * covar[index,]) + m[index])
        )
      )
    })
# output should be an rdd, indexed by row, of list(column, explinpred)
  })
  bob <- take(linpred.rdd, 1L)
  print("linpred debug")
  print(str(bob))
  beta.rdd <- combineByKey(linpred.rdd, createCombiner = function(v) {
    # output of this should be an rdd, indexed by row, of the content of that row
    row <- rep(0, length.out = V)
    row[v$column] <- v$explinpred
  }, 
  function(C, v) {
    C[v$column] <- v$explinpred
  }, 
  "+", 
  A)

bob <- take(beta.rdd, 1L)
print("beta recombine debug")
print(str(bob))

#  beta <- explinpred/rowSums(explinpred)
#   beta <- split(beta, rep(1:A, each=K))
  beta.rdd <-flatMap(beta.rdd, function(x) {
    index <- 0
    alply(x[[2]]/sum(x[[2]]), .margins = 1, .fun = function(y) {
      index <<- index + 1
      list(aspect = (((index - 1) * A * K) + y[[1]]) %% K, # which cell in the total array, mod K
           list(
             row = x[[1]],
             column = index, 
             y))
    }
    )
    #output should be an rdd, indexed by aspect, of list(row, column, value)
  })
  
bob <- take(beta.rdd, 1L)
print("beta rdd row sums debug")
print(str(bob))

  #wrangle into the list structure
#  beta <- lapply(beta, matrix, nrow=K)

  beta.rdd <- combineByKey(beta.rdd, createCombiner = function(v) {
    ret <- matrix(0, nrow = K, ncol = V)
    ret[v$row, v$column] <- ret[[3]] # This is not right, but we haven't figured out how to do it yet and it can stand in for now
    ret
  }, 
  function(C, v) {
    C[v$row, v$column] <- v[[3]]
    C
  }, 
  "+", 
  A)
cache(beta.rdd)
bob <- take(beta.rdd, 1L)
print("beta output debug")
print(str(bob))

# kappa <- split(coef, row(coef)) # NEED TO DEAL WITH THIS -- WILL FAIL RIGHT NOW
  kappa <- list(m=m, params=kappa)
  out <- list(kappa=kappa, nlambda=nlambda, beta.rdd = beta.rdd)
  return(out)
}

# This is the old code for transposing the beta list of matrices
# # beta.transpose.rdd should at this point be a set of A * V matrices, 
# # where each matrix is (K * 3).  The first two columns are the index number
# # of the aspect, and the row number from the original beta.combined.rdd matrix
# beta.transpose.rdd <- map(beta.combined.rdd, function(x) {
#   print("transpose map")
#   print(str(x))
#   list(#aspect = x[[1]], 
#     column = 1, 
#     value = cbind(x[[1]], 1:nrow(x[[2]]), x[[2]][,1])
#   )
# })
# print("Union loop")
# while (col.i < V) {
#   col.i <- col.i + 1
#   beta.transpose.rdd <- unionRDD(beta.transpose.rdd, map(beta.combined.rdd, function(x) {
#     ret <- list(#aspect = x[[1]], 
#       column = col.i, 
#       value = cbind(x[[1]], 1:nrow(x[[2]]), x[[2]][,col.i])
#     )
#     print("union")
#     print(str(ret))
#     ret
#   }))
# }
# print("beta combine")
# # At this point, there are A entries in the rdd for each term.  Now merge them by rbind.
# # The result will be V matrices, each one K * 3.  The first two columns are the aspect
# # index and row index of that row.
# cache(beta.transpose.rdd) # for debugging
# bob <- reduce(beta.transpose.rdd, c) # the only purpose of this is to force completion for debugging purposes
# beta.transpose.rdd <- combineByKey(beta.transpose.rdd, 
#                                    createCombiner = function(v) {
#                                      print("create combiner")
#                                      print(str(v)) 
#                                      v},
#                                    mergeValue = rbind, 
#                                    mergeCombiners = rbind,
#                                    numPartitions = V)
# print("map value beta")
# beta.transpose.rdd <- mapValues(beta.transpose.rdd, function(x) {
#   print(str(x))
#   x[order(x[,1], x[,2]),c(-1,-2)]
# })
# #  transposed.length <- count(beta.transpose.rdd)

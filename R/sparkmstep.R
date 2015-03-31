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
  
 # counts <- beta.ss
  
  covar.broadcast <- settings$covar.broadcast
  m.broadcast <- settings$m.broadcast
  
  mult.nobs <- br #number of multinomial draws in the sample
  offset <- log(mult.nobs)
  offset.broadcast <- broadcast(spark.context, offset)
  
  # it is cheaper to re-distribute counts as an RDD each pass through the loop than it would be to 
  # convert the matrix from rows to columns.  This is something potentially worth revisiting
  # for very large data sets.
#   counts <- split(counts, col(counts)) # now a list, indexed by term, of arrays
#   index <- 0
#   counts.list <- lapply(counts, function(x) {
#     index <<- index + 1
#     list(
#       t = index, 
#       c.i = x#, 
#       #      m.i = ifelse(is.null(m), NULL, m[index])
#     )
#   })
#   bf <- paste0(settings$betafile, round(rnorm(1) * 10000))
#   saveAsObjectFile(parallelize(spark.context, counts.list, spark.partitions), bf)
#   counts.rdd <- objectFile(spark.context, bf, spark.partitions)
#   rm(counts.list)
  
  counts.rdd <- groupByKey(flatMap(filterRDD(hpb.rdd, function(x) x[[1]] == "betacolumns"), 
                                   function(x) x[[2]]), as.integer(spark.partitions))
  
  #########
  #Distributed Poissons
  #########
  
  verbose <- settings$verbose
  
  mnreg.rdd <- mapPartitionsWithIndex(counts.rdd, function(split, part) {
    # each part should be a column from counts, representing a single vocabulary term
    offset.in <- value(offset.broadcast)
    covar.in <- value(covar.broadcast)
    i.start <- part[[1]][[1]]
    m <- value(m.broadcast)
    
    coef <- sapply(part, USE.NAMES=FALSE,simplify=TRUE,function(a.count) {
      i <- a.count[[1]]
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

#    coef <- do.call(cbind,map.out)
    coef <- covar.in %*% coef # should have one column for each vocab in input, and rows = nrow(covar.in)
                              # there should be A*K rows
    assert_that(nrow(coef) == A*K)

    coef <- matrix(coef)

    coef <- sweep(coef, 2, 
                  STATS=m[i.start:(i.start + ncol(coef) - 1)], FUN="+")
    coef <- exp(coef)
    # want to split into one matrix per aspect.  First split into list of aspects, one vector each
    coef <- split(coef, rep(1:A, K)  )

    index <- 0
    lapply(coef, function(x) {
      index <<- as.integer(index + 1)
      list(aspect = index, 
           list(col = i.start, x = x))
    })
  })
  
  mnreg.rdd <- groupByKey(mnreg.rdd, as.integer(A))
  mnreg.rdd <- mapPartitionsWithIndex(mnreg.rdd, function(split, part) {
    lapply(part, function(x) {
      aspect <- x[[1]]
      C <- matrix(rep(0, V * K), nrow = K)
      lapply(x[[2]], function(v) {
        y <- matrix(v[[2]], nrow = K)
        C[,v[[1]]:(v[[1]] + ncol(y) - 1) ] <<- y
      })
      list(aspect, C/rowSums(C))
    })
  })
  
  # We're collecting this only so we can broadcast it again because we haven't figured out a memory-efficient
  # way to join beta.rdd and documents.rdd.  
  beta <- collectAsMap(mnreg.rdd)
  beta.distributed <- distribute.beta(spark.context = spark.context, beta, spark.partitions)
  unpersist(hpb.rdd)
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


#
# This function has not been tested - there's no point until we have a viable theory of how to do 
# opt sigma in the cluster, which as of now we don't.  
#
opt.mu.spark <- function(lambda, mode=c("CTM","Pooled", "L1"), settings) {
#   if (mode == "L1") return(stm:::opt.mu(lambda, mode, covar, enet))
#   if (mode == "CTM") return(matrix(colMeans(lambda), ncol=1))
  index <- 0
  # need to turn lambda into columns -- which is interesting since we need lambda to be rows for opt sigma
  lambda.rdd <- NULL
  if (! "RDD" %in% class(lambda)) {
    lambda.out <- apply(lambda, MARGIN=2, function(x) {
      index <<- index + 1
      list(as.integer(index), x)
    })
    fn <- paste0(settings$mufile, round(rnorm(1) * 1000))
    print("to parallel")
    print(str(lambda.out))
    lambda.rdd <- parallelize(settings$spark.context, lambda.out)
    print("in parallel")
    saveAsObjectFile(lambda.rdd, fn)
    lambda.rdd <- objectFile(spark.context, fn, settings$spark.partitions)
  } else {
    # convert the hpb chunks into column chunks
    lambda.rdd <- flatMap(lambda, function(x) {
      l <- x[[2]]$l #all columns, some rows, first column is a row index
      startrow <- l[1,1]
      l <- l[,-1]
      index <- 0
      apply(l, MARGIN=2, function(y) {
        index <<- index + 1
        list(
          as.integer(index), #column
          list(
            startrow = startrow, 
            y
          )
        )
      })
    })
    # combine the column chunks
    lambda.rdd <- groupByKey(hpb.rdd)
  }
  
  covar <- settings$X.broadcast
  
  mapPartitionsWithIndex(lambda.rdd, function(split, part) {
    covar.in <- value(covar)
    lapply(part, function(a.lambda) {
      colindex <- a.lambda[[1]]
      if ("list" %in% class(a.lambda[[2]])) {
        # combine column chunks into columns
      } else {
        column <- a.lambda[[2]]
      }
      list(colindex, 
           covar.in %*% vb.variational.reg(Y=column, X = covar.in)
      )
    }) 
  })
}

#
# This function is not complete -- it should work partially, 
# but we haven't yet figured out a good way to do the crossprod.
# Also reallly should think this through.  Since it wants lambda as an .rdd of rows, 
# we could loop it through documents.rdd, or through hpb.rdd (persisting that). 
# But we need to solve sigma and broadcast it since we need all of sigma in the e-step.  So, to think about. 
#
opt.sigma.spark <- function(sigma.ss, lambda, mu, settings) {  
  sigprior <- settings$sigma$prior
  # Assume that lambda is an rdd, and mu is an rdd unless its only one column
  onecol <- FALSE # is mu only 1 column?
  if (onecol) {
    covariance <- mapValues(lambda, function(x) {
      # This could easily be converted to run off of either documents.rdd or hpb.rdd.  Probably hpb, since
      # then we could partition by index
      x - as.numeric(mu)
    })
  } else {
    covariance.rdd <- join(lambda, mu, spark.partitions = as.integer(settings$spark.partitions))
    # This has to be run off of documents.rdd, or hpb.rdd has to be split into rows.  Because mu is
    # separate columns.
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
  )
  covar.rows.rdd <- flatMap(covar.rows.rdd, function(x) {
    out <- list()
    for (i in 1:(K-1)) {
      out[[i]] <- list(coltomatch = i, 
           list(row = x[[1]], 
                x[[2]]))
    }
  })
  crossprod.rdd <- rightOuterJoin(covar.rows.rdd, covariance.rdd, as.integer(spark.partitions))
  crossprod.rdd <- mapPartitions(crossprod.rdd, function(part) {
    out <- matrix(rep(0, (K-1)^2), nrow = K - 1)
    lapply(part, function(x) {
      out.column <- x[[1]]
      in.column <- x[[2]][[2]]
      out.row <- x[[2]][[1]]$row
      in.row <- x[[2]][[1]]$x
      out[out.row, in.row] <<- sum(in.column * in.row)
    })
  })
  
  # now need to take the crossproduct of covariance.rdd
  # then figure out how to do the below...
  sigma <- (covariance + nu)/nrow(lambda) #add to estimation variance
  sigma <- diag(diag(sigma),nrow=nrow(nu))*sigprior + (1-sigprior)*sigma #weight by the prior
  return(sigma)
}






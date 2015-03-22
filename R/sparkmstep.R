# getting rid of no-fixed-intercept
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
  
  mnreg.rdd <- mapPartitionsWithIndex(counts.rdd, function(split, part) {
    # each part should be a column from counts, representing a single vocabulary term
    offset.in <- value(offset.broadcast)
    covar.in <- value(covar.broadcast)
    i.start <- part[[1]]$t
    m <- value(m.broadcast)
    
    map.out <- sapply(part, USE.NAMES=FALSE,simplify=FALSE,function(a.count) {
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
    
 #   coef <- do.call(cbind,map.out)
    coef <- covar.in %*% coef
    coef <- sweep(coef, 2, STATS=m[i.start:(i.start + ncol(coef) - 1)], "+")
    coef <- exp(coef)
    index <- 0
    apply(coef, 1, function(x) {
      index <<- index + 1
      
      list(
        aspect = 1 + ((index - 1) %% A), 
        list(row = ceiling(index / A), 
             col = i.start,
             x
        )
      )
    })
  }) # output should be chunks of what will become the beta list of matrices.  
  
  mnreg.rdd <- combineByKey(mnreg.rdd, createCombiner = function(v) {
    C <- matrix(rep(0, V * K), nrow = K) 
    C[v[["row"]],v[["col"]]:(v[["col"]] + length(v[["x"]])-1) ] <- v[["x"]]
    C
  }, mergeValue = function(C, v) {
    C[v[["row"]],v[["col"]]:(v[["col"]] + length(v[["x"]])-1) ] <- v[["x"]]
    C
  }, mergeCombiners = `+`, 
  A
  ) 
  
#   mnreg.rdd <- mapValues(mnreg.rdd, function(x) {
#     m <- value(m.broadcast)
#     x <- sweep(x, 2, STATS = m, FUN = "+")
#     x <- exp(x)
#     x <- x / rowSums(x)
#   })
  beta <- collectAsMap(mnreg.rdd)
  beta <- lapply(beta, function(x) {x / rowSums(x)})
  # beta calculates, but beta is a key,value pair list, not just a list now
  beta.distributed <- distribute.beta(spark.context = spark.context, beta, spark.partitions)
  
  list(beta = beta, nlambda=nlambda, beta.distributed = beta.distributed)
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
  
  linpred <- as.matrix(covar%*%coef) 
  
  linpred <- sweep(linpred, 2, STATS=settings$m, FUN="+")
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
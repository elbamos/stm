#Workhorse Function for the STM model
#compared to the original we have more initializations, 
# more explicit options, trimmed fat, memoization

stm.control <- function(documents, vocab, settings, model, spark.context, spark.partitions = NULL) {
  
  globaltime <- proc.time()
  verbose <- settings$verbose
  ##########
  #Step 1: Initialize Parameters
  ##########
  ngroups <- settings$ngroups
  if(is.null(model)) {
    if(verbose) cat("Beginning Initialization.\n")
    #initialize
    model <- stm:::stm.init(documents, settings)
    #unpack
    mu <- list(mu=model$mu)
    sigma <- model$sigma
    beta <- list(beta=model$beta)
    if(!is.null(model$kappa)) beta$kappa <- model$kappa
    lambda <- model$lambda
    convergence <- NULL 
    #discard the old object
    rm(model)
  } else {
    if(verbose) cat("Restarting Model...\n")
    #extract from a standard STM object so we can simply continue.
    mu <- model$mu
    beta <- list(beta=lapply(model$beta$logbeta, exp))
    if(!is.null(model$beta$kappa)) beta$kappa <- model$beta$kappa
    sigma <- model$sigma
    lambda <- model$eta
    convergence <- model$convergence
    #manually declare the model not converged or it will stop after the first iteration
    convergence$stopits <- FALSE
    convergence$converged <- FALSE
    #iterate by 1 as that would have happened otherwise
    convergence$its <- convergence$its + 1 
  }    
  
  #Pull out some book keeping elements
  ntokens <- sum(settings$dim$wcounts$x)
  betaindex <- settings$covariates$betaindex
  stopits <- FALSE
  

    includePackage(spark.context, "glmnet")
    includePackage(spark.context, "plyr")
    includePackage(spark.context, "Matrix")
    # if we change documents to have a key as the first element, then we can use an RDD
    if (is.null(names(documents))) names(documents) <- 1:length(documents)
    doc.keys <- names(documents)
    index <- 0
    doclist <- llply(documents, .fun = function(x) {
      index <<- index + 1
      list(key = betaindex[index], 
           list(key = doc.keys[index], 
                doc.num = index,
                document = x,
                lambda = lambda[index,],
                aspect = betaindex[index])
      )
    })
    documents.rdd <- parallelize(spark.context, doclist, spark.partitions)
      
    beta.distributed <- distribute.beta(beta$beta, spark.context, spark.partitions) 
    mu <- distribute.mu(mu, spark.context)
    
  #The covariate matrix
  rows <- settings$dim$A * settings$dim$K
  if(settings$dim$A==1) { #Topic Model
    covar <- diag(1, nrow=settings$dim$K)
  }
  if(settings$dim$A!=1) { #Topic-Aspect Models
    #Topics
    veci <- 1:rows
    vecj <- rep(1:settings$dim$K,settings$dim$A)
    #aspects
    veci <- c(veci,1:rows)
    vecj <- c(vecj,rep((settings$dim$K+1):(settings$dim$K+settings$dim$A), each=settings$dim$K))
    if(settings$kappa$interactions) {
      veci <- c(veci, 1:rows)
      vecj <- c(vecj, (settings$dim$K+settings$dim$A+1):(settings$dim$K+settings$dim$A+rows))
    }
    vecv <- rep(1,length(veci))
    covar <- sparseMatrix(veci, vecj, x=vecv)
  }  
  settings$covar <- covar
  settings$covar.broadcast <- broadcast(spark.context, covar)  
  
  
  
    if (doDebug) print("Distributed initial rdd's")
  ############
  #Step 2: Run EM
  ############
  while(!stopits) {
        t1 <- proc.time()
        if (verbose) cat("Distributing E-Step\t")
        documents.old.rdd <- documents.rdd
        documents.rdd <- estep.spark.better( 
          documents.rdd = documents.rdd,
          N = length(documents),
          V = length(vocab),
          beta.distributed = beta.distributed,
          mu = mu, 
          sigma = sigma, 
          spark.context = spark.context,
          spark.partitions = spark.partitions,
          verbose) 
        persist(documents.rdd, "MEMORY_AND_DISK_SER_2") #OFF_HEAP") 
#        ,"MEMORY_ONLY_SER")
#        checkpoint(documents.rdd)
        if (doDebug) print("persisted off heap")


        if (doDebug) print("Mapping beta.")
        rm(beta.unreduced.rdd)
        beta.unreduced.rdd <- map(documents.rdd, function(x) {
          list(
                key = x[[2]]$aspect, 
                beta.slice = x[[2]]$beta.slice
                )
          }
          )
        if (doDebug) print("Reducing beta.")
        rm(beta.combined.rdd)
        A <- settings$dim$A
        beta.combined.rdd <- reduceByKey(beta.unreduced.rdd,  "+", 
                                         min(spark.partitions, A)
                                        ) # need to fix number of partitions

        if (is.null(beta$kappa)) {
          beta.ss <- collect(beta.combined.rdd)[[1]]
          if (doDebug) print(str(beta.ss))
          beta.ss <- beta.ss/rowSums(beta.ss)
          beta.distributed <- broadcast(spark.context, beta.ss)
          # beta is not being collected here
          beta$beta <- beta.ss
          beta$beta.distributed <- beta.distributed
        }  else {
          if(settings$tau$mode=="L1") {
            if (doDebug) print("mnreg.")
             beta <- mnreg.spark(beta.combined.rdd, settings, spark.context, spark.partitions)
             beta.distributed <- beta$beta.distributed
          } else {
            if (doDebug) print("Reducing beta for jefreys kappa.")
            beta.ss <- collect(beta.combined.rdd) 
            beta <- stm:::jeffreysKappa(beta.ss, kappa, settings) 
            beta$beta.distributed <- distribute.beta(beta = beta, spark.context, spark.partitions)
            beta.distributed <- beta$beta.distributed
          }
        }
        if (doDebug) print("Mapping lambda")
        lambda.rdd <- map(documents.rdd, function(x) {c(x[[2]]$doc.num, x[[2]]$lambda)})
        if (doDebug) print("Reducing lambda")
        lambda <- reduce(lambda.rdd, rbind)
        if(verbose) {
          cat(sprintf("E-Step And Opt Betas and Lambda Definitely Completed Within (%d seconds).\n", floor((proc.time()-t1)[3])))
          t1 <- proc.time()
        }
        if(doDebug) print(str(lambda))
        lambda <- lambda[order(lambda[,1]),]
        lambda <- lambda[,-1]
        if (doDebug) print("Opt mu")
        mu.local <- stm:::opt.mu(lambda=lambda, mode=settings$gamma$mode, 
               covar=settings$covariates$X, settings$gamma$enet)
        if (doDebug) print(str(mu.local))
        mu <- distribute.mu(mu.local, spark.context, spark.partitions)
        if (doDebug) print("Extract sigma")
        sigma.extract.rdd <- mapValues(documents.rdd, function(x) {x$sigma}) 
        sigma.ss <- reduce(sigma.extract.rdd, function(x, y) {
          if ("list" %in% class(x)) x <- x[[2]]
          if ("list" %in% class(y)) y <- y[[2]]
          x + y
        })
if (doDebug) print("Opt sigma")
        sigma <- stm:::opt.sigma(nu=sigma.ss, lambda=lambda, 
               mu=mu.local$mu, sigprior=settings$sigma$prior)
        
        bound.extract.rdd <- mapValues(documents.rdd, function(x) {c(x$doc.num, x$bound)})
        bound.ss <- reduce(bound.extract.rdd, function(x, y) {
          if ("list" %in% class(x)) x <- x[[2]]
          if ("list" %in% class(y)) y <- y[[2]]
          rbind(x, y)
        }) 
        bound.ss <- bound.ss[order(bound.ss[,1]),-1]
        bound.ss <- as.vector(bound.ss)
        if (verbose) cat(sprintf("Completed M-Step (%d seconds). \n", floor(proc.time()-t1)[3]))
        
    #Convergence
    convergence <- stm:::convergence.check(bound.ss, convergence, settings)
    stopits <- convergence$stopits

    #Print Updates if we haven't yet converged
    # The report function won't work properly if there's no content covariate because beta hasn't been recovered
    if(!stopits & verbose) stm:::report(convergence, ntokens=ntokens, beta, vocab, 
                                       settings$topicreportevery, verbose)
    unpersist(documents.old.rdd)
  }

  #######
  #Step 3: Construct Output
  #######
  time <- (proc.time() - globaltime)[3]
  #convert the beta back to log-space
  beta$logbeta <- beta$beta
  for(i in 1:length(beta$logbeta)) {
    beta$logbeta[[i]] <- log(beta$logbeta[[i]])
  }
  beta$beta <- NULL
  beta$beta.distributed <- NULL
  lambda <- cbind(lambda,0)
  model <- list(mu=mu.local, sigma=sigma, beta=beta, settings=settings,
                vocab=vocab, convergence=convergence, 
                theta=exp(lambda - stm:::row.lse(lambda)), 
                eta=lambda[,-ncol(lambda), drop=FALSE],
                invsigma=solve(sigma), time=time, version=utils::packageDescription("stm")$Version)
  class(model) <- "STM"  
  return(model)
}



    



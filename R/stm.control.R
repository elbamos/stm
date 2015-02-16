#Workhorse Function for the STM model
#compared to the original we have more initializations, 
# more explicit options, trimmed fat, memoization

stm.control <- function(documents, vocab, settings, model, spark.context) {
  
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
  

#     includePackage(spark.context, "glmnet")
    # if we change documents to have a key as the first element, then we can use an RDD
    if (is.null(names(documents))) names(documents) <- 1:length(documents)
    doc.keys <- names(documents)
    index <- 0
    doclist <- llply(documents, .fun = function(x) {
      index <<- index + 1
      list(key = doc.keys[index], 
           list(key = doc.keys[index], 
                doc.num = index,
                document = x,
                lambda = lambda[index,],
                aspect = betaindex[index])
      )
    })
    names(doclist) <- doc.keys
    documents.rdd <- parallelize(spark.context, doclist)
    rm(doclist)
      
    beta.rdd <- distribute.beta(beta$beta, spark.context) 
    
  ############
  #Step 2: Run EM
  ############
  while(!stopits) {
        t1 <- proc.time()
        if (verbose) cat("Distributing E-Step\t")
        documents.rdd <- estep.spark.better( 
          documents.rdd = documents.rdd,
          N = length(documents),
          V = length(vocab),
          beta.rdd = beta.rdd,
          mu = mu, 
          sigma = sigma, 
          spark.context = spark.context,
          verbose) 

#         sigma.ss <- suffstats$sigma
#         lambda <- suffstats$lambda
#         beta.ss <- suffstats$beta
#         bound.ss <- suffstats$bound
#         #do the m-step
#         beta <- opt.beta(beta.ss, beta$kappa, settings)
#         mu <- opt.mu(lambda=lambda, mode=settings$gamma$mode, 
#                      covar=settings$covariates$X, settings$gamma$enet)
#         sigma <- opt.sigma(nu=sigma.ss, lambda=lambda, 
#                            mu=mu$mu, sigprior=settings$sigma$prior)
        cache(documents.rdd)
        print("Mapping beta.")
        beta.unreduced.rdd <- map(documents.rdd, function(x) {
          list(
                key = x$doc.results$aspect, 
                beta.slice = x$doc.results$beta.slice)
          }
          )
        print("Combining beta.")
        beta.combined.rdd <- combineByKey(beta.unreduced.rdd, 
                                          createCombiner = function (v) {v}
                                          , mergeValue = "+" 
                                          , mergeCombiners = "+" , 1L) # need to fix number of partitions

        if (is.null(beta$kappa)) {
          print("Reducing beta.")
          beta.rdd <- mapValues(beta.combined.rdd, reduce.beta.nokappa) # there's only one aspect...
        }  else {
          if(settings$tau$mode=="L1") {
            beta.ss <- collect(beta.combined.rdd, flatten = TRUE)
            beta <- stm:::mnreg(beta.ss,settings)
#             beta <- mnreg.spark(beta.combined.rdd, settings)
#             beta.rdd <- distribute.beta(spark.context, beta.combined.rdd)
          } else {
            beta.ss <- collect(beta.combined.rdd, flatten = TRUE)
            beta <- stm:::jeffreysKappa(beta.ss, kappa, settings) 
          }
        }
        print("Mapping lambda")
        lambda.rdd <- map(documents.rdd, function(x) {x$doc.results$lambda.output})
        print("Reducing lambda")
        lambda <- reduce(lambda.rdd, rbind)
        if(verbose) {
          cat(sprintf("Completed E-Step (%d seconds). \n", floor((proc.time()-t1)[3])))
          t1 <- proc.time()
          cat("Starting M-Step. ")
        }
        lambda <- lambda[order(lambda[,1]),]
        lambda <- lambda[,-1]
        print("Opt mu")
        mu <- stm:::opt.mu(lambda=lambda, mode=settings$gamma$mode, 
               covar=settings$covariates$X, settings$gamma$enet)
        print("Mapping sigma")
        sigma.extract.rdd <- map(documents.rdd, function(x) {x$doc.results$eta$nu}) 
        print("Reducing sigma")
        sigma.ss <- reduce(sigma.extract.rdd, "+")
        print("Opt sigma")
        sigma <- stm:::opt.sigma(nu=sigma.ss, lambda=lambda, 
               mu=mu$mu, sigprior=settings$sigma$prior)
        
        print("Mapping bound")
        bound.extract.rdd <- map(documents.rdd, function(x) {x[[2]]$bound.output})
        print("Reducing bound")
        bound.ss <- reduce(bound.extract.rdd, rbind) 
        print("Have the bound")
        bound.ss <- bound.ss[order(bound.ss[,1]),-1]
        bound.ss <- as.vector(bound.ss)
        if (verbose) cat(sprintf("Completed M-Step (%d seconds). \n", floor(proc.time()-t1)[3]))
        
    #Convergence
    convergence <- stm:::convergence.check(bound.ss, convergence, settings)
    stopits <- convergence$stopits

    #Print Updates if we haven't yet converged
    # The report function won't work properly because we don't have beta at this point -- confirm this is the reason
    if(!stopits & verbose) stm:::report(convergence, ntokens=ntokens, beta, vocab, 
                                       settings$topicreportevery, verbose)
  }
  #######
  #Step 3: Construct Output
  #######
  # Need to recover beta here
  time <- (proc.time() - globaltime)[3]
  #convert the beta back to log-space
  beta$logbeta <- beta$beta
  for(i in 1:length(beta$logbeta)) {
    beta$logbeta[[i]] <- log(beta$logbeta[[i]])
  }
  beta$beta <- NULL
  lambda <- cbind(lambda,0)
  model <- list(mu=mu, sigma=sigma, beta=beta, settings=settings,
                vocab=vocab, convergence=convergence, 
                theta=exp(lambda - row.lse(lambda)), 
                eta=lambda[,-ncol(lambda), drop=FALSE],
                invsigma=solve(sigma), time=time, version=utils::packageDescription("stm")$Version)
  class(model) <- "STM"  
  return(model)
}



    



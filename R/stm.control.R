#Workhorse Function for the STM model
#compared to the original we have more initializations, 
# more explicit options, trimmed fat, memoization

stm.control.spark <- function(documents, vocab, settings, model, 
                              spark.context, spark.partitions, 
                              spark.persistence, spark.filename) {
  
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
    if(!is.null(model$kappa)) 
      settings$bkappa <- model$kappa
    lambda <- model$lambda
    convergence <- NULL 
    #discard the old object
    rm(model)
  } else {
    if(verbose) cat("Restarting Model...\n")
    #extract from a standard STM object so we can simply continue.
    mu <- model$mu
    beta <- list(beta=lapply(model$beta$logbeta, exp))
    if(!is.null(model$beta$kappa)) settings$bkappa <- model$beta$kappa # note this won't restart properly
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
  
  if (doDebug || verbose) print("Distributing globals.")
  # sensible default partitioning
  if (is.null(spark.partitions)) spark.partitions <- as.integer(4 * round(log(settings$dim$N * settings$dim$K * settings$dim$A)))
  includePackage(spark.context, "glmnet")
  includePackage(spark.context,"Matrix")
  includePackage(spark.context, "assertthat")
#  includePackage(spark.context, "plyr")

  index <- 0
  doclist <- lapply(documents, FUN = function(x) {
    index <<- index + 1
    list(as.integer(index), list( 
      dn = as.integer(index),
      d = x,
      a = as.integer(betaindex[index]),
      nd = sum(x[2,]),
      l = lambda[index,]
  ))
  })

  # documents.rdd is a pair list.  Each iteration, 
  # the e-step runs logisticnormal inside mapPartitions to update lambda.
  # This produces a new documents.rdd, which is persisted.
  # Then, hpb is executed through a mapPartitions, to produce hpb.rdd with sufficient stats,
  # and with beta and lambda separated into columns.  sigma.ss, the rowSums of beta.ss, and 
  # the sum of the bounds, are recovered from hpb.rdd.  
  # The columns of lambda are extracted from hpb.rdd and processed into a mu.rdd.  
  # mu.rdd is joined with the old documents.rdd, the result is persisted, and the old documents.rdd is unpersisted.
  # Opt-sigma is run by subtracting mu from lambda in documents.rdd, the results are recovered to complete
  # opt-sigma, and the new siginv and sigmaentropy are broadcast.  
  # Then, the columns of beta.ss are extracted from hpb.rdd.  Mnreg is run on the columns to produce an
  # mnreg.rdd which is indexed by aspect.  The aspects are merged, the resulting beta matrix recovered,
  # and beta is re-broadcast.  The process the begins again with documents.rdd.

  # beta, mu, sigma, and siginv are broadcast variables.  
  filename <- paste0(spark.filename, round(abs(rnorm(1) * 10000)), ".rdd")
  saveAsObjectFile(parallelize(spark.context, doclist, spark.partitions), filename)
  documents.rdd <- objectFile(spark.context, filename, spark.partitions)
  
  beta.distributed <- distribute.beta(beta$beta, spark.context, spark.partitions) 
  mu.distributed <- distribute.mu(mu, spark.context, spark.partitions = spark.partitions, settings)
  lambda.distributed <- distribute.lambda(lambda, spark.context, spark.partitions)


  if(!is.null(settings$bkappa) && settings$tau$mode=="L1") {
    # If we are going to be running mnreg, broadcast the covariate matrix since
    # its a global.
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
    settings$covar.broadcast <- broadcast(spark.context, covar)
    settings$covar <- covar
      
    m <- settings$dim$wcounts$x
    m <- log(m) - log(sum(m))
    settings$m.broadcast <- broadcast(spark.context, m)
    settings$m <- m
  }
  X <-  settings$covariates$X
  settings$X.broadcast <- broadcast(spark.context, X)
  settings$spark.partitions <- spark.partitions
  settings$spark.persistence <- spark.persistence
  
  sigmaentropy <- (.5*determinant(sigma, logarithm=TRUE)$modulus[1])
  siginv <- solve(sigma)
  siginv.broadcast <- broadcast(spark.context, siginv)
  sigmaentropy.broadcast <- broadcast(spark.context, sigmaentropy)
  
  if (doDebug) print("Distributed initial rdd's")
  ############
  #Step 2: Run EM
  ############
  hpb.rdd <- NULL
#  iteration <- 0
  while(!stopits) {
#    iteration <<- iteration + 1
#   settings$iteration <- broadcast(spark.context, iteration)
    t1 <- proc.time()
    cat("Beginning E-Step\t")
    
    # siginv and sigmaentropy have to be broadcast each pass through the loop

      documents.rdd <- estep.lambda( 
        documents.rdd,
        beta.distributed,
        mu.distributed, 
        siginv.broadcast,
        spark.context,
        spark.partitions,
        settings,
        verbose)
      persist(documents.rdd, spark.persistence)

      estep.output <- estep.hpb( 
        length(vocab), 
        settings$dim$K,
        settings$dim$A,
        documents.rdd,
        beta.distributed,
        mu.distributed, 
        siginv.broadcast,
        sigmaentropy.broadcast,
        spark.context,
        spark.partitions,
        verbose)
      if (! is.null(hpb.rdd)) unpersist(hpb.rdd)
      hpb.rdd <- estep.output$hpb.rdd
    

    if(verbose) {
      cat(sprintf("E-Step Completed Within (%d seconds).\n", floor((proc.time()-t1)[3])))
      t1 <- proc.time()
    }

    mu.distributed <- opt.mu.spark(hpb.rdd, mode = settings$gamma$mode, settings)

    sigma.ss <- estep.output$s
    sig.list <- opt.sigma.spark(nu=sigma.ss, documents.rdd, 
                             mu.distributed, settings)
    sigma <- sig.list[[1]]
    documents.rdd <- sig.list[[2]]
    
    sigmaentropy <- (.5*determinant(sigma, logarithm=TRUE)$modulus[1])
    siginv <- solve(sigma)
    siginv.broadcast <- broadcast(spark.context, siginv)
    sigmaentropy.broadcast <- broadcast(spark.context, sigmaentropy)
  
    if (is.null(settings$bkappa)) {
      beta.ss <- reduceByKey(hpb.rdd, function(x) {
           vectorcombiner(x)
      })
      beta.ss <- do.call(rbind, beta.ss)
      beta.ss <- beta.ss / rowSums(beta.ss)
      beta.distributed <- distribute.beta(beta = list(beta.ss), spark.context = spark.context, spark.partitions = spark.partitions)
      beta$beta <- beta.ss
      beta$beta.distributed <- beta.distributed
    }  else {
      assert_that(settings$tau$mode == "L1")
      beta <- mnreg.spark.distributedbeta(hpb.rdd, estep.output$br, settings, spark.context, spark.partitions)
      beta.distributed <- beta$beta.distributed
    }
    bound.ss <- estep.output$bd  

    if (verbose) cat(sprintf("Completed M-Step (%d seconds). \n", floor(proc.time()-t1)[3]))
    #Convergence
    convergence <- stm:::convergence.check(bound.ss, convergence, settings)
    stopits <- convergence$stopits
    
    #Print Updates if we haven't yet converged
    # The report function won't work properly if there's no content covariate because beta hasn't been recovered
    if(!stopits & verbose) stm:::report(convergence, ntokens=ntokens, beta, vocab, 
                                        settings$topicreportevery, verbose)
    #unpersist(old)
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
  lambda.rdd <- mapValues(documents.rdd, function(x) x[[1]]$l)
  mu.rdd <- mapValues(documents.rdd, function(x) x[[2]])
  lambda <- collectAsMap(lambda.rdd)
  lambda <- lambda[order(names(lambda))]
  lambda <- do.call(rbind, lambda)
  # Need to collect beta.ss here
  mu <- collectAsMap(mu.rdd)
  mu <- mu[order(names(mu))]
  mu <- do.call(cbind, mu)
  lambda <- cbind(lambda,0)
  
  saveAsObjectFile(documents.rdd, paste0(spark.filename, "documents.rdd"))
  unpersist(hpb.rdd)
  unpersist(documents.rdd)
  
  model <- list(mu=mu, sigma=sigma, beta=beta, settings=settings,
                vocab=vocab, convergence=convergence, 
                theta=exp(lambda - stm:::row.lse(lambda)), 
                eta=lambda[,-ncol(lambda), drop=FALSE],
                sufficient.statws = list(sigma.ss = sigma.ss), 
                documents.rdd = paste0(spark.filename, "documents.rdd")
                invsigma=solve(sigma), time=time, version=utils::packageDescription("stm")$Version)
  class(model) <- "STM"  
  return(model)
}







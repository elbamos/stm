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
#  includePackage(spark.context, "plyr")

  index <- 0
  doclist <- lapply(documents, FUN = function(x) {
    index <<- index + 1
    list( 
      dn = index,
      d = x,
      a = as.integer(betaindex[index]),
      nd = sum(x[2,])
  )
  })
  if (estages == 2) {
    doclist <- lapply(doclist, FUN = function(x) {
      x$l <- lambda[x$dn,]
      x
    })
  }
  # documents.rdd is a value list (not a pair list).  Each iteration, 
  # the e-step runs logisticnormal inside mapPartitionsAsIndex to update lambda.
  # This produces a new documents.rdd, which is persisted in memory and on disk.
  # (This should really be changed to be user controllable.)
  # Then, hpb is executed through a mapPartitionsAsIndex, and the output
  # from each partition is merged.  All partitions' outputs are merged in a reduce.
  # Then, the prior iteration's documents.rdd is unpersisted, and the m-step is executed.
  
  # beta, mu, sigma, and siginv are broadcast variables.  It may be worth exploring
  # whether beta or mu could be implemented as rdd's which are then joined with documents.rdd
  # during the e-step. This would probably require accumulators, which have not 
  # yet been implemented in SparkR, to be efficient.
  filename <- paste0(spark.filename, round(abs(rnorm(1) * 10000)), ".rdd")
  saveAsObjectFile(parallelize(spark.context, doclist, spark.partitions), filename)
  documents.rdd <- objectFile(spark.context, filename, spark.partitions)
  persist(documents.rdd, spark.persistence)
  settings$betafile <- paste0(spark.filename, round(abs(rnorm(1) * 10000)), ".rdd")
  settings$mufile <- paste0(spark.filename, round(abs(rnorm(1) * 10000)), ".rdd")
  
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
    settings$covar <- covar
    settings$covar.broadcast <- broadcast(spark.context, covar)  
    m <- settings$dim$wcounts$x
    m <- log(m) - log(sum(m))
    settings$m.broadcast <- broadcast(spark.context, m)
    settings$m <- m
  }
  if (doDebug) print("Distributed initial rdd's")
  ############
  #Step 2: Run EM
  ############
  while(!stopits) {
    t1 <- proc.time()
    cat("Beginning E-Step\t")
    
    # siginv and sigmaentropy have to be broadcast each pass through the loop
    sigmaentropy <- (.5*determinant(sigma, logarithm=TRUE)$modulus[1])
    siginv <- solve(sigma)
    siginv.broadcast <- broadcast(spark.context, siginv)
    sigmaentropy.broadcast <- broadcast(spark.context, sigmaentropy)
    if ("DIST_M" %in% mstep) {
      documents.rdd <- join(documents.rdd, mu.distributed, as.integer(spark.partitions))
    }
    if (estages == 1) {
      estep.output <- estep.spark(
        documents.rdd = documents.rdd,
        A = settings$dim$A,
        K = settings$dim$K,
        V = length(vocab),
        beta.distributed = beta.distributed,
        mu = mu.distributed, 
        lambda.distributed = lambda.distributed,
        siginv.broadcast = siginv.broadcast,
        sigmaentropy.broadcast = sigmaentropy.broadcast,
        spark.context = spark.context,
        spark.partitions = spark.partitions,
        verbose)
    } else {
      old <- documents.rdd
      documents.rdd <- estep.lambda( 
        documents.rdd,
        beta.distributed,
        mu.distributed, 
        siginv.broadcast,
        spark.context,
        spark.partitions,
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
      unpersist(old)
    }

    if(verbose) {
      cat(sprintf("E-Step Completed Within (%d seconds).\n", floor((proc.time()-t1)[3])))
      t1 <- proc.time()
    }

    lambda <- estep.output$l
    if (estages == 1) lambda.distributed <- distribute.lambda(lambda, spark.context, spark.partitions)
    mu.local <- stm:::opt.mu(lambda=lambda, mode=settings$gamma$mode, 
                             covar=settings$covariates$X, settings$gamma$enet)
    mu.distributed <- distribute.mu(mu.local, spark.context, spark.partitions)
    sigma.ss <- estep.output$s
    sigma <- stm:::opt.sigma(nu=sigma.ss, lambda=lambda, 
                             mu=mu.local$mu, sigprior=settings$sigma$prior)
    
    if (is.null(settings$bkappa)) {
      beta.ss <- rbind(estep.output$b)[[1]]
      beta.ss <- beta.ss / rowSums(beta.ss)
      beta.distributed <- distribute.beta(beta = list(beta.ss), spark.context = spark.context, spark.partitions = spark.partitions)
      beta$beta <- beta.ss
      beta$beta.distributed <- beta.distributed
    }  else {
      if(settings$tau$mode=="L1") {
        if ("DIST_B" %in% mstep){
          beta <- mnreg.spark.distributedbeta(estep.output$b, settings, spark.context, spark.partitions)
        } else {  
          beta <- mnreg.spark(estep.output$b, settings, spark.context, spark.partitions)
        }
        beta.distributed <- beta$beta.distributed
      } else {
        beta <- stm:::jeffreysKappa(estep.output$b, kappa, settings) 
        beta$beta.distributed <- distribute.beta(beta = beta$beta, spark.context, spark.partitions)
        beta.distributed <- beta$beta.distributed
      }
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
  }
  unpersist(documents.rdd)
  
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







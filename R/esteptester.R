# K <- 20
# 
# 
# #Generate the objects necessary to run e-step
# 
# library(stm)
# library(Matrix)
# library(matrixStats)
# library(slam)
data(poliblog5k)
documents <- poliblog5k.docs
vocab <- poliblog5k.voc
# A <- 1
# V <- length(vocab)
# N <- length(documents)
# wcountvec <- unlist(lapply(documents, function(x) rep(x[1,], times=x[2,])),use.names=FALSE)
# wcounts <- list(Group.1=sort(unique(wcountvec)))
# V <- length(wcounts$Group.1)  
# wcounts$x <- tabulate(wcountvec)
# rm(wcountvec)
# verbose <- TRUE
# reportevery <- TRUE
# emtol <- 1e-5
# xmat <- NULL
# betaindex <- rep(1,length(documents))
# yvarlevels <- NULL
# 
# settings <- list(dim=list(K=K, A=A, 
#                           V=V, N=N, wcounts=wcounts),
#                  verbose=verbose,
#                  topicreportevery=reportevery,
#                  convergence=list(max.em.its=max.em.its, em.converge.thresh=emtol),
#                  covariates=list(X=xmat, betaindex=betaindex, yvarlevels=yvarlevels),
#                  sigma=list(prior=0),
#                  kappa=list(LDAbeta=TRUE),
#                  init=list(mode="Spectral", nits=50, burnin=25, alpha=(50/K), eta=.01), 
#                  seed=02138,
#                  ngroups=1)
# settings$gamma$mode <- "CTM"
# 
# globaltime <- proc.time()
# verbose <- settings$verbose
# ngroups <- settings$ngroups
# 
# stm.init <- stm:::stm.init
# if(verbose) cat("Beginning Initialization.\n")
# #initialize
# model <- stm.init(documents, settings)
# #unpack
# mu <- list(mu=model$mu)
# sigma <- model$sigma
# beta <- list(beta=model$beta)
# if(!is.null(model$kappa)) beta$kappa <- model$kappa
# lambda <- model$lambda
# convergence <- NULL 
# #discard the old object
# rm(model)
# 
# #Pull out some book keeping elements
# ntokens <- sum(settings$dim$wcounts$x)
# betaindex <- settings$covariates$betaindex
# stopits <- FALSE
# suffstats <- vector(mode="list", length=ngroups)
# 
# 
# 
# 
# 
# 
# 
# #######
# # Compare
# #######
# #optionally switch out for your version here
# estep <- stm:::estep
# 
# model <- stm.init(documents, settings)
# t1 <- proc.time()
# #run the model
# suffstats <- estepdebugger(documents=documents, beta.index=betaindex,  update.mu=(!is.null(mu$gamma)),  beta$beta, lambda, mu$mu, sigma, verbose)
# msg <- sprintf("Completed E-Step (%f seconds). \n", (proc.time()-t1)[3])
# msg
# 
# 

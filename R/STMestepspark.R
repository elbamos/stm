# #E-Step for a Document Block
# #[a relatively straightforward rewrite of previous
# # code with a focus on avoiding unnecessary computation.]
# 
# #Input: Documents and Key Global Parameters
# #Output: Sufficient Statistics
# 
# # Approach:
# # First we pre-allocate memory, and precalculate where possible.
# # Then for each document we:
# #  (1) get document-specific priors, 
# #  (2) infer doc parameters, 
# #  (3) update global sufficient statistics
# # Then the sufficient statistics are returned.
# 
# #Let's start by assuming its one beta and we may have arbitrarily subset the number of docs.
# sparkContext <- sparkR.init(master = "local", appName = "estep2")
# includePackage(sparkContext, "stm")
# includePackage(sparkContext, "Matrix")
# includePackage(sparkContext, "matrixStats")
# includePackage(sparkContext, "splines")
# includePackage(sparkContext, "slam")
# includePackage(sparkContext, "lda")
# includePackage(sparkContext, "glmnet")
# includePackage(sparkContext, "stringr")
# # if we change documents to have a key as the first element, then we can use an RDD
# documents.broadcast <- broadcast(sparkContext, documents) 
# #documents.rdd <- parallelize(sparkContext, documents)
# beta.index.broadcast <- broadcast(sparkContext, beta.index)
# 
# 
# #sufftest <- estep.spark(update.mu=(!is.null(mu$gamma)),  beta$beta, lambda, mu$mu, sigma, sparkContext, verbose)
# 
# estep.spark <- function( 
#   update.mu, #null allows for intercept only model  
#   beta, 
#   lambda.old, 
#   mu, 
#   sigma, 
#   sparkContext,
#   verbose) {
#   ctevery <- ifelse(N>100, floor(N/100), 1)
#   if(!update.mu) mu.i <- as.numeric(mu)
#   
#   # 1) Initialize Sufficient Statistics 
#   sigma.ss <- diag(0, nrow=(K-1))
#   beta.ss <- vector(mode="list", length=A)
#   for(i in 1:A) {
#     beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
#   }
#   bound <- vector(length=N)
#   lambda <- vector("list", length=N)
#   
#   # 2) Precalculate common components
#   sigmaentropy <- (.5*determinant(sigma, logarithm=TRUE)$modulus[1])
#   siginv <- solve(sigma)
#   
#   #broadcast what we need
#   sigmaentropy.broadcast <- broadcast(sparkContext, sigmaentropy)
#   siginv.broadcast <- broadcast(sparkContext, siginv)
#   
#   # Make a list
#   estep.rdd <- list()
#   for (i in 1:4) { # This should be to N, and we should change it to use document names once that has been put in
#     aspect <- beta.index[i]
#     words <- documents[[i]][1,]
#     docdata <- list(
#       key = i,
#       init = lambda.old[i,], 
#       beta.i = beta[[aspect]][,words,drop=FALSE],
#       mu.i =  mu.i# ifelse(update.mu, mu[,i], mu.i)
#     )
#     estep.rdd[[i]] <- docdata
#   }
#   estep.rdd <- parallelize(sparkContext, estep.rdd)
#   # If I make documents an RDD, should I run cogroup here?
#   resultRDD <- lapply(estep.rdd, function(x) {
#     i <- x[[1]]
#     doc <- value(documents.broadcast)[[i]]
# #    doc <- lookup(documents.rdd, i)[[1]]
#     siginv <- value(siginv.broadcast)
#     sigmaentropy <- value(sigmaentropy.broadcast)
#     init <- x[[2]]
#     mu.i <- x[[4]]
#     beta.i <- x[[3]]
#     doc.results <- stm:::logisticnormal(eta = init, 
#                                    mu = mu.i, 
#                                    siginv = siginv,
#                                    beta = beta.i, 
#                                    doc = doc,
#                                    sigmaentropy  = sigmaentropy
#                                    )
#     list(key = i, doc.results = doc.results)
#   }
#   )
#   
#   results <- collect(resultRDD, flatten = FALSE)[[1]]
#   for (i in 1:4) { # this should be an lapply, but whatever
#     key <- results[[i]]$key
#     doc.results <- results[[i]]$doc.results
#     words <- documents[[i]][1,]
#     aspect <- beta.index[i]
#     sigma.ss <- sigma.ss + doc.results$eta$nu
#     aspect <- beta.index[i]
#     beta.ss[[aspect]][,words] <- doc.results$phis + beta.ss[[aspect]][,words]
#     bound[i] <- doc.results$bound
#     lambda[[i]] <- doc.results$eta$lambda
#     if(verbose && i%%ctevery==0) cat(".")
#   }
#   # Here is where to deal with any errors processing the results of the map
# 
# 
#   if(verbose) cat("\n") #add a line break for the next message.
#   
#   #4) Combine and Return Sufficient Statistics
#   lambda <- do.call(rbind, lambda)
#   return(list(sigma=sigma.ss, beta=beta.ss, bound=bound, lambda=lambda))
# }
# 
# sufftest <- estep.spark(update.mu=(!is.null(mu$gamma)),  beta$beta, lambda, mu$mu, sigma, sparkContext, verbose)
# 
# 
# suffstats <- estepdebugger(documents=documents, beta.index=betaindex,  update.mu=(!is.null(mu$gamma)),  beta$beta, lambda, mu$mu, sigma, verbose)
# 
# 
# 
# 
# 
# 
# estepdebugger <- function(documents, beta.index, update.mu, #null allows for intercept only model  
#                   beta, lambda.old, mu, sigma, 
#                   verbose) {
#   
#   #quickly define useful constants
#   V <- ncol(beta[[1]])
#   K <- nrow(beta[[1]])
#   N <- length(documents)
#   A <- length(beta)
#   ctevery <- ifelse(N>100, floor(N/100), 1)
#   if(!update.mu) mu.i <- as.numeric(mu)
#   
#   # 1) Initialize Sufficient Statistics 
#   sigma.ss <- diag(0, nrow=(K-1))
#   beta.ss <- vector(mode="list", length=A)
#   for(i in 1:A) {
#     beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
#   }
#   bound <- vector(length=N)
#   lambda <- vector("list", length=N)
#   
#   # 2) Precalculate common components
#   sigmaentropy <- (.5*determinant(sigma, logarithm=TRUE)$modulus[1])
#   siginv <- solve(sigma)
#   
#   # 3) Document Scheduling
#   # For right now we are just doing everything in serial.
#   # the challenge with multicore is efficient scheduling while
#   # maintaining a small dimension for the sufficient statistics.
#   for(i in 1:4) {
#     #update components
#     doc <- documents[[i]]
#     words <- doc[1,]
#     aspect <- beta.index[i]
#     init <- lambda.old[i,]
#     if(update.mu) mu.i <- mu[,i]
#     beta.i <- beta[[aspect]][,words,drop=FALSE]
#     paste("Document ", i, length(doc), names(doc)) %>% print()
#     paste("sigmainv", length(siginv), str(siginv)) %>% print()
#     paste("sigmaentropy", length(sigmaentropy), names(sigmaentropy)) %>% print()
#     paste("mu.i", length(mu.i), str(mu.i)) %>% print()
#     paste("beta.i", length(beta.i), str(beta.i)) %>% print()
#     #infer the document
#     doc.results <- stm:::logisticnormal(eta=init, mu=mu.i, siginv=siginv, beta=beta.i, 
#                                   doc=doc, sigmaentropy=sigmaentropy)
#     
#     print("results")
#     paste("etanu", str(doc.results$eta$nu)) %>% print()
#     print("lambda", str(doc.results$eta.lambda)) %>% print()
#     
#     # update sufficient statistics 
#     sigma.ss <- sigma.ss + doc.results$eta$nu
#     beta.ss[[aspect]][,words] <- doc.results$phis + beta.ss[[aspect]][,words]
#     bound[i] <- doc.results$bound
#     lambda[[i]] <- doc.results$eta$lambda
#     if(verbose && i%%ctevery==0) cat(".")
#   }
#   if(verbose) cat("\n") #add a line break for the next message.
#   
#   #4) Combine and Return Sufficient Statistics
#   lambda <- do.call(rbind, lambda)
#   return(list(sigma=sigma.ss, beta=beta.ss, bound=bound, lambda=lambda))
# }
# 
# 

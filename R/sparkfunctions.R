# sparkobject <- list(
#   sc = spark.context, 
#   docfilename = , 
#   covar.broadcast = ,
#   m.broadcast = , 
#   documents.rdd = ,
#   
# )

lhood <- function(eta, doc.ct, mu, siginv,beta, Ndoc) {
  expeta <- c(exp(eta),1)
  part1 <- sum(doc.ct*log(.colSums(beta*expeta, nrow(beta), ncol(beta)))) - Ndoc*log(sum(expeta))
  # -1/2 (eta - mu)^T Sigma (eta - mu)
  diff <- eta-mu
  part2 <- .5*sum(diff*(siginv %*% diff))
  part2 - part1  
} 

grad <- function(eta, doc.ct, mu, siginv, beta, Ndoc) {
  expeta.sh <- exp(eta) 
  expeta <- c(expeta.sh,1)
  Ez <- expeta*beta
  denom <- doc.ct/.colSums(Ez, nrow(Ez), ncol(Ez))
  part1 <- (Ez%*%denom)[-length(expeta)] - expeta.sh*(Ndoc/sum(expeta))  
  part2 <- siginv%*%(eta-mu) 
  as.numeric(part2 - part1)
}

merge.beta <- function(x, y) {
  for (i in 1:length(x)) x[[i]] <- x[[i]] + y[[i]]
  x
}

distribute.beta <- function(beta, spark.context, spark.partitions) {
  broadcast(sc = spark.context, beta)
}

distribute.lambda <- function(lambda, spark.context, spark.partitions) {
  broadcast(sc = spark.context, lambda)
}

distribute.mu <- function(mu, spark.context, spark.partitions, settings) {
  if (ncol(mu$mu) > 1) {
    mf <- paste0(settings$mufile, round(rnorm(1) * 10000))
    index <- 0
    mu <- apply(mu$mu, MARGIN=2, function(x) {
      index <<- index + 1
      list(as.integer(index), 
           x)
    })
#    parallelize(spark.context, mu, spark.partitions)
    saveAsObjectFile(parallelize(spark.context, mu, spark.partitions), mf)
    objectFile(spark.context, mf, spark.partitions)
  } else {
  mu <- mu$mu
  broadcast(spark.context, mu)#)
  }
}

# Hessian/Phi/Bound
#   NB: Hessian function is not as carefully benchmarked as it isn't called
#       nearly as often.  Particularly I suspect some of the elements in the
#       cross product could be sped up quite a bit.
#   NB: Bound and hessian barely communicate with one another here.
hpb <- function(eta, doc.ct, mu, siginv, beta, Ndoc=sum(doc.ct), sigmaentropy) {
  #basic transforms
  expeta <- c(exp(eta),1)
  theta <- expeta/sum(expeta)
  
  #pieces for the derivatives of the exp(eta)beta part
  EB <- expeta*beta #calculate exp(eta)\beta for each word
  EB <- t(EB)/colSums(EB) #transpose and norm by (now) the row
  
  #at this point EB is the phi matrix
  phi <- EB*(doc.ct) #multiply through by word count
  phisums <- colSums(phi)
  phi <- t(phi) #transpose so its in the K by W format expected
  EB <- EB*sqrt(doc.ct) #set up matrix to take the cross product
  
  #First piece is the quotient rule portion that shows up from E[z], second piece is the part
  # that shows up regardless as in Wang and Blei (2013) for example.  Last element is just siginv
  hess <- -((diag(phisums) - crossprod(EB)) - 
              Ndoc*(diag(theta) - theta%o%theta))[1:length(eta),1:length(eta)] + siginv
  
  ###
  # Bound
  
  nu <- try(chol2inv(chol.default(hess)), silent=TRUE)
  if(class(nu)=="try-error") {
    #brute force solve
    nu <- solve(hess)
    #only if we would produce negative variances do we bother doing nearPD
    if(any(diag(nu)<0)) nu <- as.matrix(nearPD(nu)$mat)
  }
  diff <- eta - mu
  logphinorm <- log(colSums(theta*beta))
  part1 <- sum(doc.ct*logphinorm)
  bound <- part1 + .5*determinant(nu, logarithm=TRUE)$modulus -
    .5*sum(diff*crossprod(diff,siginv)) -
    sigmaentropy
  bound <- as.numeric(bound)
  
  #bundle everything up.
  return(list(phis=phi, eta=list(lambda=eta, nu=nu), bound=bound))
}

vectorcombiner <- function(x) {
  x <- Reduce(x = x, f = function(x, y) list(c(x[[1]], y[[1]]), c(x[[2]], y[[2]])))
  x[[2]][order(x[[1]])]
}
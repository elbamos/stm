# processes documents.rdd, running logisticnormal and 
# producing a new documents.rdd with an updated lambda
estep.lambda <- function( 
  documents.rdd,
  beta.distributed,
  mu.distributed, 
  siginv.broadcast,
  settings) {
  
  mapPartitions(documents.rdd, function(part) {
    if ("Broadcast" %in% class(mu.distributed)) mu.in <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    siginv.in <- value(siginv.broadcast)
    assert_that(length(part) > 0)
    lapply(part, function(document) {
      document <- document[[2]]
      # Handle if documents.rdd has been joined to mu.rdd
      if (length(document) == 2) {
        mu.i <- vectorcombiner(document[[2]])
        document <- document[[1]][[1]]
        document[["mu.i"]] <- mu.i
      }
      
      if ("Broadcast" %in% class(mu.distributed)) {
        if (ncol(mu.in) > 1) {
          mu.i <- mu.in[,document[["dn"]]]
        } else {
          mu.i <- as.numeric(mu.in)
        }
        document[["mu.i"]] <- mu.i
      }
      
      beta.i.lambda <- beta.in[[document[["a"]]]][,document[["d"]][1,],drop=FALSE]

      document[["l"]] <- optim(par=document[["l"]], fn=lhood, gr=grad,
                          method="BFGS", control=list(maxit=500),
                          doc.ct=document[["d"]][2,], mu=mu.i,
                          siginv=siginv.in, beta=beta.i.lambda, Ndoc = document[["nd"]])$par
      list(document[["dn"]], document)
    })
  })
}


estep.hpb <- function( 
  V, 
  K,
  A,
  documents.rdd,
  beta.distributed,
  siginv.broadcast,
  sigmaentropy.broadcast) {
  
  # loops through partitions of documents.rdd, collecting sufficient stats per-partition.   Produces
  # a pair (key, value) RDD where the key is the partition and the value the sufficient stats.
  hpb.rdd <- mapPartitions(documents.rdd, function(part) {
    beta.ss <- vector(mode="list", length=A)
    for(i in 1:A) {
      beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
    }
    sigma.ss <- diag(0, nrow=(K-1))

    beta.in <- value(beta.distributed)
    siginv.in <- value(siginv.broadcast)
    sigmaentropy.in <- value(sigmaentropy.broadcast)
    assert_that(length(part) > 0)
    bound <- 0
    lambda <- sapply(part, USE.NAMES=FALSE,FUN=function(document) {
      document <- document[[2]]
      mu.i <- document[["mu.i"]]
      words <- document[["d"]][1,]

      #Solve for Hessian/Phi/Bound returning the result
      doc.results <- hpb(document[["l"]], doc.ct=document[["d"]][2,], mu=mu.i,
                         siginv=siginv.in, beta=beta.in[[document[["a"]]]][,words,drop=FALSE], document[["nd"]] ,
                         sigmaentropy=sigmaentropy.in)
      
      beta.ss[[document[["a"]]]][,words] <<- doc.results[["phis"]] + beta.ss[[document[["a"]]]][,words]
      sigma.ss <<- sigma.ss + doc.results[["eta"]][["nu"]]
      bound <<- bound + doc.results[["bound"]]
      c(document[["dn"]], document[["l"]])
    })
    beta.ss <- do.call(rbind, beta.ss)
    br <- rowSums(beta.ss) # doing this now lets us perform mnreg in the cluster without bringing all of beta back in
    
    index <- 0
    betaout <- apply(beta.ss, MARGIN=2, FUN= function(x) {
      index <<- index + 1
      if (sum(x) == 0) {NULL} 
      else {list(as.integer(index), x)}
    })
    betaout <- Filter(Negate(is.null), betaout)
    
    index <- 0
    lr <- lambda[1,]
    lambda <- lambda[2:nrow(lambda),]
    lambdaout <- apply(lambda, MARGIN=1, FUN=function(x) {
      index <<- index + 1
      list(as.integer(index), list(lr, x))
    })

    list(list("o", list(
      s = sigma.ss, 
      bd = sum(bound),
      br = br
    )), 
    list("b", betaout), 
    list("l", lambdaout))
  })

  cache(hpb.rdd)
  
  # Recover sufficient stats
  out <- reduce(filterRDD(hpb.rdd, function(x) x[[1]] == "o"), function(x, y) {
    if ((is.null(x) || is.integer(x)) && !is.null(y)) return(y)
    if ((is.null(y) || is.integer(y)) && !is.null(x)) return(x)
    if (length(x) == 2) x <- x[[2]]
    if (length(y) == 2) y <- y[[2]]
    assert_that(length(x) == 3, length(x) == length(y))
      list(bd = x[["bd"]] + y[["bd"]], 
           s = x[["s"]] + y[["s"]], 
           br = x[["br"]] + y[["br"]]
      )
  })

  out[["hpb.rdd"]] <- hpb.rdd
  out
}

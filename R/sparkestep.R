# processes documents.rdd, running logisticnormal and 
# producing a new documents.rdd with an updated lambda
estep.lambda <- function( 
  documents.rdd,
  beta.distributed,
  mu.distributed, 
  siginv.broadcast,
  spark.context,
  spark.partitions,
  settings,
  verbose) {
  
  #
  # If mu is not being turned into an rdd, use 0.  If mu is being turned into an rdd,
  # it will still be broadcast on the first iteration so use 1.  If its already an rdd, use 2. 
  #

    if ("Broadcast" %in% class(mu.distributed)) {
      mstage <- 1
    } else {
      mstage <- 2
    }
  iteration <- settings$iteration
  K <- settings$dim$K
  
  mapPartitionsWithIndex(documents.rdd, function(split, part) {
    if (mstage < 2) mu.in <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    siginv.in <- value(siginv.broadcast)
    assert_that(length(part) > 0)
    lapply(part, function(document) {
      if (mstage > 0) {
        document <- document[[2]]
        if (length(document) == 2) {
          mu.i <- document[[2]]
          document <- document[[1]]
          document$mu.i <- mu.i
        }
      }
      if (mstage < 2) {
        if (ncol(mu.in) > 1) {
          mu.i <- mu.in[,document$dn]
        } else {
          mu.i <- as.numeric(mu.in)
        }        
      }
      assert_that(length(mu.i) == K - 1)
#      if (value(iteration) == 1) stop(mu.i)
      
      beta.i.lambda <- beta.in[[document$a]][,document$d[1,],drop=FALSE]

      document$l <- optim(par=document$l, fn=lhood, gr=grad,
                          method="BFGS", control=list(maxit=500),
                          doc.ct=document$d[2,], mu=mu.i,
                          siginv=siginv.in, beta=beta.i.lambda, Ndoc = document$nd)$par
      if (mstage > 0) {
        list(document$dn, document)
      } else {
        document
      }
    })
  })
}


estep.hpb <- function( 
  V, 
  K,
  A,
  documents.rdd,
  beta.distributed,
  mu.distributed, 
  siginv.broadcast,
  sigmaentropy.broadcast,
  spark.context,
  spark.partitions,
  verbose) {
  
  mstage <- 0

    if ("Broadcast" %in% class(mu.distributed)) {
      mstage <- 1
    } else {
      mstage <- 2
    }

  
  # loops through partitions of documents.rdd, collecting sufficient stats per-partition.   Produces
  # a pair (key, value) RDD where the key is the partition and the value the sufficient stats.
  hpb.rdd <- mapPartitionsWithIndex(documents.rdd, function(split,part) {
    beta.ss <- vector(mode="list", length=A)
    for(i in 1:A) {
      beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
    }
    sigma.ss <- diag(0, nrow=(K-1))
    lambda <- rep(NULL, times = K) # K - 1, plus 1 column for row order so we can sort later
    
    if (mstage < 2) mu.in <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    siginv.in <- value(siginv.broadcast)
    sigmaentropy.in <- value(sigmaentropy.broadcast)
    assert_that(length(part) > 0)
    bound <- 0
    lapply(part, function(document) {
      if (mstage > 0) {
        document <- document[[2]]
        mu.i <- document$mu.i
      } 
      if (mstage < 2) {
        if (ncol(mu.in) > 1) {
          mu.i <- mu.in[,document$dn]
        } else {
          mu.i <- as.numeric(mu.in)
        }        
      }
      words <- document$d[1,]

      #Solve for Hessian/Phi/Bound returning the result
      doc.results <- hpb(document$l, doc.ct=document$d[2,], mu=mu.i,
                         siginv=siginv.in, beta=beta.in[[document$a]][,words,drop=FALSE], document$nd ,
                         sigmaentropy=sigmaentropy.in)
      
      beta.ss[[document$a]][,words] <<- doc.results$phis + beta.ss[[document$a]][,words]
      sigma.ss <<- sigma.ss + doc.results$eta$nu
      lambda <<- rbind(lambda, c(document$dn, document$l))
      bound <<- bound + doc.results$bound
    })
    beta.ss <- do.call(rbind, beta.ss)
    br <- rowSums(beta.ss)
    index <- 0
#    startrow <- as.integer(lambda[1,1])
    betaout <- apply(beta.ss, MARGIN=2, FUN= function(x) {
      index <<- index + 1
      if (sum(x) == 0) {NULL} 
      else {list(as.integer(index), x)}
    })
    betaout <- Filter(Negate(is.null), betaout)
    index <- 0
    ll <- lambda[,2:ncol(lambda)]
    lambdaout <- apply(ll, MARGIN=2, FUN=function(x) {
      index <<- index + 1
      list(as.integer(index), list(lambda[,1], x))
    })
#    index <- as.integer(split/sqrt(spark.partitions))
    list(list(key = "output", list(
      s = sigma.ss, 
      bd = sum(bound),
      br = br,
      l = lambda
    )), 
    list(key = "betacolumns", betaout), 
    list(key = "lambdacolumns", lambdaout))
  })

  # merge the sufficient stats generated for each partition
  cache(hpb.rdd)
  output.rdd <- filterRDD(hpb.rdd, function(x) x[[1]] == "output")
  out <- reduce(output.rdd, function(x, y) {
    if ((is.null(x) || is.integer(x)) && !is.null(y)) return(y)
    if ((is.null(y) || is.integer(y)) && !is.null(x)) return(x)
    if (length(x) == 2) x <- x[[2]]
    if (length(y) == 2) y <- y[[2]]
    assert_that(length(x) == 4, length(x) == length(y))
      list(bd = x$bd + y$bd, 
           s = x$s + y$s, 
           l = rbind(x$l, y$l), 
           br = x$br + y$br
      )
  })

  lambda <- out$l[order(out$l[,1]),]
  out$l <- lambda[,-1]
  out$hpb.rdd <- hpb.rdd
  out
}

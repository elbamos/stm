# processes documents.rdd, running logisticnormal and 
# producing a new documents.rdd with an updated lambda
estep.lambda <- function( 
  documents.rdd,
  beta.distributed,
  mu.distributed, 
  siginv.broadcast,
  spark.context,
  spark.partitions,
  verbose) {
  
  #
  # If mu is not being turned into an rdd, use 0.  If mu is being turned into an rdd,
  # it will still be broadcast on the first iteration so use 1.  If its already an rdd, use 2. 
  #
  mstage <- 0
  if ("DIST_M" %in% mstep) {
    if ("Broadcast" %in% class(mu.distributed)) {
      mstage <- 1
    } else {
      mstage <- 2
    }
  }
  
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

hpb.combiner <- function(part.rdd) {
  if (FALSE) {
    print("count")
    toss <- SparkR::count(part.rdd)
    print("count")
  }
  if ("COMBINE" %in% reduction) {
    part.rdd <- combineByKey(part.rdd, createCombiner = function(v) v, 
                             mergeValue = function(C, v) {
                               list(
                                 bd = rbind(C$bd, v$bd), 
                                 s = C$s + v$s, 
                                 b = merge.beta(C$b, v$b), 
                                 l = rbind(C$l, v$l)
                               )
                             },
                             mergeCombiners = function(C1, C2) {
                               list(
                                 bd = rbind(C1$bd, C2$bd), 
                                 s = C1$s + C2$s, 
                                 b = merge.beta(C1$b, C2$b), 
                                 l = rbind(C1$l, C2$l)
                               )
                             },
                             numPartitions = as.integer(sqrt(spark.partitions))
    )
  }
  # This would not be bad if it knew how many cpus there were per node
  if ("REPARTITION" %in% reduction) {
    part.rdd <- coalesce(part.rdd, numPartitions = as.integer(sqrt(part)))
    part.rdd <- mapPartitionsWithIndex(part.rdd, function (split, part) {
      out <- part[[1]][[2]]
      for (index in 2:length(part)) {
        current <- part[[index]][[2]]
        out <- list(bd = rbind(out$bd, curent$bd), 
                    s = out$s + current$s, 
                    b = merge.beta(out$b, current$b), 
                    l = rbind(out$l, current$l)
        )
      }
      list(list(split, out))
    })
  }
  # try to combine using an intermediate step
  if ("KEY" %in% reduction) { # turn this on when we understand it better
    part.rdd <- reduceByKey(part.rdd, function(x, y) {
      assert_that(length(x) == 4, length(x) == length(y))
        list(bd = rbind(x$bd, y$bd), 
             s = x$s + y$s, 
             b = merge.beta(x$b, y$b), 
             l = rbind(x$l, y$l)
        )
    }, 
    numPartitions = as.integer(sqrt(spark.partitions))
    )
  }
  part.rdd
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
  if ("DIST_M" %in% mstep) {
    if ("Broadcast" %in% class(mu.distributed)) {
      mstage <- 1
    } else {
      mstage <- 2
    }
  }
  
  # loops through partitions of documents.rdd, collecting sufficient stats per-partition.   Produces
  # a pair (key, value) RDD where the key is the partition and the value the sufficient stats.
  part.rdd <- mapPartitionsWithIndex(documents.rdd, function(split,part) {
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
    bound <- sapply(part, function(document) {
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
      c(document$dn, doc.results$bound)
    })
    index <- as.integer(split/sqrt(spark.partitions))
    list(list(key = index, list(
      s = sigma.ss, 
      b = beta.ss, 
      bd = t(bound),
      l = lambda
    )))
  })
  part.rdd <- hpb.combiner(part.rdd)
  # merge the sufficient stats generated for each partition
  out <- reduce(part.rdd, function(x, y) {
    if ((is.null(x) || is.integer(x)) && !is.null(y)) return(y)
    if ((is.null(y) || is.integer(y)) && !is.null(x)) return(x)
    if (length(x) == 2) x <- x[[2]]
    if (length(y) == 2) y <- y[[2]]
    assert_that(length(x) == 4, length(x) == length(y))
      list(bd = rbind(x$bd, y$bd), 
           s = x$s + y$s, 
           b = merge.beta(x$b, y$b), 
           l = rbind(x$l, y$l)
      )
  })
  bound.ss <- out$bd[order(out$bd[,1]),]
  out$bd <- bound.ss[,2]
  lambda <- out$l[order(out$l[,1]),]
  out$l <- lambda[,-1]
  out
}

estep.spark <- function( 
  V, 
  K, 
  A,
  documents.rdd,
  beta.distributed,
  mu.distributed, 
  siginv.broadcast,
  sigmaentropy.broadcast,
  lambda.distributed,
  spark.context,
  spark.partitions,
  verbose) {
  
  ss.rdd <- mapPartitionsWithIndex(documents.rdd, function(split, part) {

    beta.ss <- vector(mode="list", length=A)
    for(i in 1:A) {
      beta.ss[[i]] <- matrix(0, nrow=K,ncol=V)
    }
    sigma.ss <- diag(0, nrow=(K-1))
    lambda <- list() # K - 1, plus 1 column for row order so we can sort later
    
    if (! "DIST_M" %in% mstep) mu.in <- value(mu.distributed)
    beta.in <- value(beta.distributed)
    siginv.in <- value(siginv.broadcast)
    sigmaentropy.in <- value(sigmaentropy.broadcast)
    lambda.in <- value(lambda.distributed)

    bound <- sapply(part, function(document) {

      if ("DIST_M" %in% mstep) {
        document <- document[[2]]
        if (length(document) == 2) {
          mu.i <- document[[2]]
          document <- document[[1]]
          document$mu.i <- mu.i
        }
      }
      if ("Broadcast" %in% class(mu.distributed)) {
        if (ncol(mu.in) > 1) {
          mu.i <- mu.in[,document$dn]
        } else {
          mu.i <- as.numeric(mu.in)
        }        
      }
      
      init <- lambda.in[document$dn,]
      words <- document$d[1,]
      beta.i <- beta.in[[document$a]][,words,drop=FALSE]

      eta <- optim(par=init, fn=lhood, gr=grad,
                          method="BFGS", control=list(maxit=500),
                          doc.ct=document$d[2,], mu=mu.i,
                          siginv=siginv.in, beta=beta.i, Ndoc = document$nd)$par
      
      #Solve for Hessian/Phi/Bound returning the result
      doc.results <- hpb(eta, doc.ct=document$d[2,], mu=mu.i,
                         siginv=siginv.in, beta=beta.i, document$nd ,
                         sigmaentropy=sigmaentropy.in)
      
      beta.ss[[document$a]][,words] <<- doc.results$phis + beta.i
      sigma.ss <<- sigma.ss + doc.results$eta$nu
      lambda[[document$dn]] <<- c(document$dn, eta)
      c(document$dn, doc.results$bound)
    })
    lambda <- do.call(rbind, lambda)
    index <- as.integer(split/sqrt(spark.partitions))
    list(list(key = index, list(
      s = sigma.ss, 
      b = beta.ss, 
      bd = t(bound),
      l = lambda
    )))
  })
  ss.rdd <- hpb.combiner(ss.rdd)
  out <- reduce(ss.rdd, function(x, y) {
    if (length(x) == 2) x <- x[[2]]
    if (length(y) == 2) y <- y[[2]]
    if (length(x) == 4 && length(y) == 4) {
      list(bd = rbind(x$bd, y$bd), 
           s = x$s + y$s, 
           b = merge.beta(x$b, y$b), 
           l = rbind(x$l, y$l)
      )
    } else { 
      stop(paste("bad reduction match",
                 str(x),
                 str(y))
      )
    }
  })
  bound.ss <- out$bd[order(out$bd[,1]),]
  out$bd <- bound.ss[,2]
  lambda <- out$l[order(out$l[,1]),]
  out$l <- lambda[,-1]
  out
}
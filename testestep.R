library(plyr)
library(dplyr)
library(tm)
library(slam)
library(stringr)
library(assertthat)

library(lda)

#library(pryr)
library(Matrix)
library(matrixStats)
library(splines)
library(stm)
source("./R/sparkfunctions.R")
source("./R/stm.R")
source("./R/stm.control.R")
source("./R/STMconvergence.R")
source("./R/STMreport.R")
source("./R/sparkmstep.R")
source("./R/sparkestep.R")

#/usr/local/spark/ec2/spark-ec2 -i ~/sparkcluster.pem -k sparkcluster --instance-type=r3.xlarge --spot-price=0.04 --region=us-east-1 --zone=us-east-1e -s 5 -a ami-0a613c62 launch vanillaspark

library(SparkR)

#
# These globals are used for testing/benchmarking purposes
#

doDebug <- FALSE

ram <- "6g"
Sys.setenv(SPARK_MEM=ram)
spark.context <- NULL
filepath <- NULL
cluster <- function() {
  spark.env <- list(spark.executor.memory=ram, 
                    spark.storage.memoryFraction = "0.1",
                    spark.serializer="org.apache.spark.serializer.KryoSerializer",
                    spark.executor.extraJavaOptions="-XX:+UseCompressedOops",
                    spark.kryoserializer.buffer.mb = "512",
  driver.memory="28g",
  driver.maxResultSize='28g',
                    spark.driver.memory=ram, 
                    spark.driver.maxResultSize = ram
  )
  
  master <- system("cat /root/spark-ec2/cluster-url", intern=TRUE)
  
  spark.context <<- sparkR.init(master=master,
                               appName = paste0("poli", Sys.time()),
                               sparkEnvir=spark.env, sparkExecutorEnv = spark.env)
  filepath <- str_replace(master, "spark", "hdfs")
  filepath <<- str_replace(filepath, "7077", "9000/docs") #"hdfs://ec2-54-0-234-71.compute-1.amazonaws.com:9000/docs"
}
local <- function() {
  spark.context <<- sparkR.init("local")
  filepath <<- "/tmp/docs"
}

smalltest <- function() {
  data(gadarian)
  gadarian <- gadarian[1:100,]
  
  corpus <- textProcessor(gadarian$open.ended.response)
  prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)
  results <- stm(documents = prep$documents,
                 vocab = prep$vocab,
                 data = prep$meta, 
                 max.em.its = 5, 
                 content = ~treatment,
                 prevalence = ~ pid_rep + MetaID,
                 init.type= "Spectral", #control = list(nits=50, burnin=25, alpha=(50/20), eta=.01),
                 K = 50, spark.context = spark.context, 
                 spark.partitions = 4, 
                 spark.filename = filepath
  )
} 
mediumtest <- function() {
  data(poliblog5k)
  documents <- poliblog5k.docs
  vocab <- poliblog5k.voc
  meta <- poliblog5k.meta
  poliresults <- stm(documents = documents,
                     vocab = vocab,
                     data = meta, 
                     max.em.its = 3, 
                     content = ~rating,
                     prevalence = ~ s(day) + blog,
                     K = 100, spark.context = spark.context, 
                     init = "Spectral",
                     spark.partitions = 19, 
                     spark.filename = filepath#, 
                     #                   spark.persistence = "DISK_ONLY"
  )
  # # save(poliresuls, file="poliresults")
  # # 
}
bigtest <- function() {
  load("term_document_matrix")
  load("x_nospam")
  dtm <- as.DocumentTermMatrix(term_document_matrix)
  out <- readCorpus(dtm, type = "slam")
  names <- dplyr:::count(x, screenName)
  thresh <- 60
  names <- names[names$n > thresh,]$screenName
  x %<>% mutate(tag = factor(ifelse(is.na(tag), "unknown", as.character(tag))),
                screenName = factor(ifelse(screenName %in% names, screenName, "inactive")),
                created_numeric = as.numeric(created)
  ) %>% select(id, tag, screenName, created_numeric)
  out2 <- prepDocuments(out$documents,
                        out$vocab,
                        meta =  x[x$id %in% names(out$documents),],
                        lower.thresh = 4,
                        upper.thresh = length(out$documents) / 2, verbose = TRUE)
  rm(term_document_matrix)
  rm(x)
  rm(out)
  bigtest <- stm(documents = out2$documents,
                 vocab = out2$vocab,
                 data = out2$meta, 
                 max.em.its = 3, 
                 content = ~tag,
                 prevalence = ~screenName,
                 #                  control = list(cpp = TRUE), 
                 init.type = "Spectral",
                 K = 200, 
                 spark.context = spark.context, 
                 spark.partitions = 38, 
                 spark.filename = filepath, 
                 spark.persistence = "MEMORY_ONLY"
  )
}

#local()
cluster()

smalltest()
## mediumtest()
#bigtest()

# using 19 m1.large instances
# on one t2 instance        e-step 1200-1700        m-step 70
# Vanilla                   e-step 252              m-step 52
# -- may have been a memory/storage thing going on here.  
# 1-stage e-step            e-step 296              m-step 68
# 1-stage e-step dist b     e-step 360              m-step 89
# -- memory seemed to clear up, not sure if it was because of a SparkR revision...
# 1-stage e-step distb      e-step 284              m-step 90
# 1-stage, nodistb, 38cpu   e-step 155              m-step 82
# -- more refining of distributed beta, 38 cpus
# 1-stage distb   38cpu     e-step 156              m-step 84
# -- consolidating doc counts in documents.rdd, switch 1-2 stage, distm saves object file, sqrt(p) b partitions
# 1-stage distb   38cpu     e-step  158             m-step 97
# 2-stage nodistb 38cpu     e-step  142             m-step 53
# -- distb uses lapply instead of mapValues, and A partitions
# 2-stage distb   38cpu     e-step -                m-step 71
# -- optimized distb using groupByKey and mapPartition - also added DIST_M infrastructure
# 2-stage distb   38cpu     e-step  174             m-step  72
# 2-stage distM   38cpu     e-step  166             m-step  52
# -- switch distb to index on integers, estep to process dist_mu more quickly
# 2-stage         38cpu     e-step  144             m-step  54
# 2-stage distB   38cpu     e-step  176             m-step  75
# 2-stage distM   38cpu     e-step  165             m-step  51
# 2-stage distBM  38cpu     e-step  200             m-step  66 -- Note, this converged, which means something isn't
#                                                                 calculating right
# fully distributed m-step, minimal optimization of that.  commit 0x904f3666
# uses a 2-stage m-step, distributed mu and beta.  No distributed sigma.
# 38 cpus                   e-step  
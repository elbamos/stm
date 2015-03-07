library(plyr)
library(dplyr)
library(tm)
library(slam)

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

#/usr/local/spark/ec2/spark-ec2 -i ~/sparkcluster.pem -k sparkcluster --instance-type=r3.xlarge --spot-price=0.04 --region=us-east-1 --zone=us-east-1e -s 5 -a ami-0a613c62 launch vanillaspark

library(SparkR)

doDebug <- TRUE
reduction <- c("KEY", "COUNT") #"COMBINE" "KEY", "COLLECT", "COLLECTPARTITION", "COUNT", "REPARTITION"
# COLLECT and
# COLLECT PARTITIONS
# COUNT and COLLECT -- works up to medium size
# COLLECT ALONE -- !!! COMPLETES THE COLLECTION PHASE, FAILS AS REDUCTION (memory & disk) - but super slow
# REPARTITION -- appears to work, at least through one iteration
# COMBINE -- does successfully shrink partitions, effect on RAM not clear.  desiralization error with large data?
# KEY -- the best hope.  Function is programmatically correct. WORKS - 
#       BUT RUNS OUT OF MEMORY AT START OF 3rd ITERATION WHILE SERIALIZING THE CLOSURE.  NOTE THAT THIS WAS WITH COUNT

Sys.setenv(SPARK_MEM="10g")

spark.env <- list(spark.executor.memory="13g", 
                  spark.storage.memoryFraction = "0.1",
                  spark.serializer="org.apache.spark.serializer.KryoSerializer",
                  spark.executor.extraJavaOptions="-XX:+UseCompressedOops",
driver.memory="28g",
driver.maxResultSize='28g',
                  spark.driver.memory="10g", 
                  spark.driver.maxResultSize = "10g"
#                  spark.cores.max = 1#,
#                 ,spark.rdd.compress="true"
)

master <- system("cat /root/spark-ec2/cluster-url", intern=TRUE)

# spark.context <- sparkR.init(master=master,
#                              appName = paste0("poli", Sys.time()),
#                              sparkEnvir=spark.env, sparkExecutorEnv = spark.env)

spark.context = sparkR.init("local")

smalltest <- function() {
data(gadarian)
gadarian <- gadarian[1:100,]

corpus <- textProcessor(gadarian$open.ended.response)
prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)
results <- stm(documents = prep$documents,
               vocab = prep$vocab,
               data = prep$meta, 
               max.em.its = 3, 
                content = ~treatment,
                prevalence = ~ pid_rep + MetaID,
               init.type= "Spectral", #control = list(nits=50, burnin=25, alpha=(50/20), eta=.01),
               K = 20, spark.context = spark.context, 
               spark.partitions = 4
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
                   spark.partitions = 256, 
                   spark.persistence = "DISK_ONLY"
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
                   spark.partitions = 10
)
}
# #sparkR.stop()
#mediumtest()
bigtest()

library(stm)
library(lda)
library(plyr)
library(Matrix)
library(matrixStats)
source("./R/sparkfunctions.R")
source("./R/stm.R")
source("./R/stm.control.R")
source("./R/STMconvergence.R")
source("./R/STMreport.R")
data(gadarian)
gadarian <- gadarian[1:25,]
corpus <- textProcessor(gadarian$open.ended.response)
prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)

library(SparkR)
# spark.context <- sparkR.init("local", "estep")
doDebug = TRUE
spark.env <- list(spark.executor.memory="6g", 
                  spark.storage.memoryFraction = "0.2",
                   spark.serializer="org.apache.spark.serializer.KryoSerializer",
                   spark.executor.extraJavaOptions="-XX:+UseCompressedOops",
                  spark.driver.memory="6g", 
                  spark.driver.maxResultSize = "6g", 
                  spark.default.parallelism = 1000)
spark.context <- sparkR.init(master="spark://ec2-54-152-0-140.compute-1.amazonaws.com:7077", 
                             appName = paste0("poli", Sys.time()),
                             sparkEnvir=spark.env, sparkExecutorEnv = spark.env)
# spark.context = sparkR.init("local")
# results <- stm(documents = prep$documents,
#                vocab = prep$vocab,
#                data = prep$meta, 
#                max.em.its = 20, 
#                 content = ~treatment,
#                 prevalence = ~ pid_rep + MetaID,
#                K = 4, spark.context = spark.context, 
#                spark.partitions = 20
# )
data(poliblog5k)
documents <- poliblog5k.docs
vocab <- poliblog5k.voc
meta <- poliblog5k.meta
poliresults <- stm(documents = documents,
                   vocab = vocab,
                   data = meta, 
                   max.em.its = 200, 
                    content = ~rating,
#                    prevalence = ~ s(day) + blog,
                   K = 5, spark.context = spark.context, # K is what kills heap space
                   spark.partitions = 1000
)
save(poliresuls, file="poliresults")



#sparkR.stop()

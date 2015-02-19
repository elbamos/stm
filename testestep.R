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
# data(gadarian)
# #gadarian <- gadarian[1:25,]
# corpus <- textProcessor(gadarian$open.ended.response)
# prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)

library(SparkR)
# spark.context <- sparkR.init("local", "estep")
doDebug = TRUE
spark.context <- sparkR.init(master="spark://ec2-54-84-165-134.compute-1.amazonaws.com:7077", appName = "poli",
            sparkEnvir=list(spark.executor.memory="4g", 
                            spark.storage.memoryFraction = "0.1",
                            spark.serializer="org.apache.spark.serializer.KryoSerializer",
                            spark.executor.extraJavaOptions="-XX:+UseCompressedOops"))
# results <- stm(documents = prep$documents,
#                vocab = prep$vocab,
#                data = prep$meta, 
#                max.em.its = 20, 
#                 content = ~treatment,
#                 prevalence = ~ pid_rep + MetaID,
#                K = 4, spark.context = spark.context, 
#                spark.partitions = 10
# )
data(poliblog5k)
documents <- poliblog5k.docs
vocab <- poliblog5k.voc
meta <- poliblog5k.meta
poliresults <- stm(documents = documents,
               vocab = vocab,
               data = meta, 
               max.em.its = 200, 
#               content = ~rating,
#               prevalence = ~ s(day) + blog,
               K = 10, spark.context = spark.context, 
               spark.partitions = 200
)
save(poliresuls, file="poliresults")



sparkR.stop()

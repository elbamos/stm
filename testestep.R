library(stm)
library(lda)
library(plyr)
library(Matrix)
library(matrixStats)
source("~/stm/R/sparkfunctions.R")
source("~/stm/R/stm.R")
source("~/stm/R/stm.control.R")
source("~/stm/R/STMconvergence.R")
source("~/stm/R/STMreport.R")
data(gadarian)
gadarian <- gadarian[1:25,]
corpus <- textProcessor(gadarian$open.ended.response)
prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)

library(SparkR)
spark.context <- sparkR.init("local", "estep")
# spark.context <- sparkR.init(master="spark://ec2-52-0-98-103.compute-1.amazonaws.com:7077", appName = "stm",
#             sparkEnvir=list(spark.executor.memory="1g", 
#                             spark.eventLog.enabled="true" ))
results <- stm(documents = prep$documents,
               vocab = prep$vocab,
               data = prep$meta, 
               max.em.its = 3, 
                content = ~treatment,
                prevalence = ~ pid_rep + MetaID,
               K = 4, spark.context = spark.context
)
sparkR.stop()
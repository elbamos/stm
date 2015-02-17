library(stm)
library(lda)
library(plyr)
library(Matrix)
library(matrixStats)
source("~/stm/R/sparkfunctions.R")
source("~/stm/R/stm.R")
source("~/stm/R/stm.control.R")
data(gadarian)
gadarian <- gadarian[1:25,]
corpus <- textProcessor(gadarian$open.ended.response)
prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)

library(SparkR)
spark.context <- sparkR.init("local", "estep")
results <- stm(documents = prep$documents,
               vocab = prep$vocab,
               data = prep$meta, 
               max.em.its = 2, 
               content = ~treatment,
               K = 4, spark.context = spark.context
)
sparkR.stop()
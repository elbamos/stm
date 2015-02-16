library(stm)
library(lda)
library(plyr)
source("~/stm/R/sparkfunctions.R")
source("~/stm/R/stm.R")
source("~/stm/R/stm.control.R")
data(gadarian)
gadarian <- gadarian[1:50,]
corpus <- textProcessor(gadarian$open.ended.response)
prep <- prepDocuments(corpus$documents, corpus$vocab, corpus$meta)

library(SparkR)
spark.context <- sparkR.init("local", "estep")
results <- stm(documents = prep$documents,
               vocab = prep$vocab,
               data = prep$meta, 
               max.em.its = 2, 
               K = 20, spark.context = spark.context
)
sparkR.stop()
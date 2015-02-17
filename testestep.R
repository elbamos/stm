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
results <- stm(documents = prep$documents,
               vocab = prep$vocab,
               data = prep$meta, 
               max.em.its = 20, 
#                content = ~treatment,
#                prevalence = ~ pid_rep + MetaID,
               K = 4, spark.context = spark.context
)
sparkR.stop()
library(stm)
library(lda)
library(plyr)
library(pryr)
library(Matrix)
library(matrixStats)
source("./R/sparkfunctions.R")
source("./R/stm.R")
source("./R/stm.control.R")
source("./R/STMconvergence.R")
source("./R/STMreport.R")
#/usr/local/spark/ec2/spark-ec2 -i ~/sparkcluster.pem -k sparkcluster --instance-type=c3.xlarge --spot-price=0.04 --region=us-east-1 --zone=us-east-1e -s 15 -a ami-8e0352e6 launch vanillaspark


library(SparkR)

# spark.env <- list(spark.executor.memory="2500m", 
#                   spark.storage.memoryFraction = "0.2",
#                   spark.serializer="org.apache.spark.serializer.KryoSerializer",
#                   spark.executor.extraJavaOptions="-XX:+UseCompressedOops",
#                   spark.driver.memory="2500m", 
#                   spark.driver.maxResultSize = "2500m"
# #                 ,spark.rdd.compress="true"
# )

master <- system("cat /root/spark-ec2/cluster-url", intern=TRUE)

spark.context <- sparkR.init(master=master,
                             appName = paste0("poli", Sys.time()),
                             sparkEnvir=spark.env, sparkExecutorEnv = spark.env)
doDebug <- FALSE
# spark.context = sparkR.init("local")
# 
# 
#  data(gadarian)
# gadarian <- gadarian[1:25,]
# # 
# corpus <- textProcessor(gadarian$open.ended.response)
# prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)
# results <- stm(documents = prep$documents,
#                vocab = prep$vocab,
#                data = prep$meta, 
#                max.em.its = 1, 
#                 content = ~treatment,
#                 prevalence = ~ pid_rep + MetaID,
#                init.type="Spectral",
#                K = 10, spark.context = spark.context, 
#                spark.partitions = 2
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
                    prevalence = ~ s(day) + blog,
                   K = 50, spark.context = spark.context, 
                   init = "Spectral",
                   spark.partitions = 36
)
# save(poliresuls, file="poliresults")
# 
# # load("term_document_matrix")
# # load("x_nospam")
# # library(dplyr)
# # library(tm)
# # library(slam)
# # dtm <- as.DocumentTermMatrix(term_document_matrix)
# # out <- readCorpus(dtm, type = "slam")
# # names <- count(x, screenName)
# # thresh <- 60
# # names <- names[names$n > thresh,]$screenName
# # x %<>% mutate(tag = factor(ifelse(is.na(tag), "unknown", as.character(tag))),
# #               screenName = factor(ifelse(screenName %in% names, screenName, "inactive")),
# #               created_numeric = as.numeric(created)
# # ) %>% select(id, tag, screenName, created_numeric)
# # out2 <- prepDocuments(out$documents,
# #                       out$vocab,
# #                       meta =  x[x$id %in% names(out$documents),],
# #                       lower.thresh = 4,
# #                       upper.thresh = length(out$documents) / 2, verbose = TRUE)
# # rm(term_document_matrix)
# # rm(x)
# # rm(out)
# # bigtest <- stm(documents = out2$documents,
# #                    vocab = out2$vocab,
# #                    data = out2$meta, 
# #                    max.em.its = 200, 
# #                    content = ~tag,
# #                    prevalence = ~ s(created_numeric) + screenName,
# # #                   control = list(cpp = TRUE), 
# #                    init.type = "Spectral",
# #                    K = 200, spark.context = spark.context, 
# #                    spark.partitions = 15
# # )
# # 
# # #sparkR.stop()

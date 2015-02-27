library(dplyr)
library(tm)
library(slam)

library(lda)
library(plyr)
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
#/usr/local/spark/ec2/spark-ec2 -i ~/sparkcluster.pem -k sparkcluster --instance-type=c3.4xlarge --spot-price=0.4 --region=us-east-1 --zone=us-east-1e -s 5 -a ami-0a613c62 launch vanillaspark


library(SparkR)

Sys.setenv(SPARK_MEM="10g")

spark.env <- list(spark.executor.memory="28g", 
#                  spark.storage.memoryFraction = "0.2",
                  spark.serializer="org.apache.spark.serializer.KryoSerializer",
                  spark.executor.extraJavaOptions="-XX:+UseCompressedOops",
driver.memory="28g",
driver.maxResultSize='28g',
                  spark.driver.memory="28g", 
                  spark.driver.maxResultSize = "28g"
#                  spark.cores.max = 1#,
#                 ,spark.rdd.compress="true"
)

master <- system("cat /root/spark-ec2/cluster-url", intern=TRUE)

spark.context <- sparkR.init(master=master,
                             appName = paste0("poli", Sys.time()),
                             sparkEnvir=spark.env, sparkExecutorEnv = spark.env)
doDebug <- FALSE
# # spark.context = sparkR.init("local")
# # 
# # 
# data(gadarian)
# #gadarian <- gadarian[1:25,]
# 
# corpus <- textProcessor(gadarian$open.ended.response)
# prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)
# results <- stm(documents = prep$documents,
#                vocab = prep$vocab,
#                data = prep$meta, 
#                max.em.its = 200, 
#                 content = ~treatment,
#                 prevalence = ~ pid_rep + MetaID,
#                init.type= "Spectral", #control = list(nits=50, burnin=25, alpha=(50/20), eta=.01),
#                K = 20#, spark.context = spark.context, 
#               # spark.partitions = 20
# )
# data(poliblog5k)
# documents <- poliblog5k.docs
# vocab <- poliblog5k.voc
# meta <- poliblog5k.meta
# poliresults <- stm(documents = documents,
#                    vocab = vocab,
#                    data = meta, 
#                    max.em.its = 200, 
#                     content = ~rating,
#                     prevalence = ~ s(day) + blog,
#                    K = 100, spark.context = spark.context, 
#                    init = "Spectral",
#                    spark.partitions = 256
# )
# # # save(poliresuls, file="poliresults")
# # # 
load("term_document_matrix")
load("x_nospam")
dtm <- as.DocumentTermMatrix(term_document_matrix)
out <- readCorpus(dtm, type = "slam")
names <- count(x, screenName)
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
                   max.em.its = 200, 
                   content = ~tag,
                   prevalence = ~screenName,
#                  control = list(cpp = TRUE), 
                   init.type = "Spectral",
                   K = 200, 
                   spark.context = spark.context, 
                   spark.partitions = 18
)

#sparkR.stop()

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
#/usr/local/spark/ec2/spark-ec2 -i ~/sparkcluster.pem -k sparkcluster --instance-type=c3.xlarge --spot-price=0.04 --region=us-east-1 --zone=us-east-1e -s 15 -a ami-8e0352e6 launch vanillaspark


library(SparkR)

spark.env <- list(spark.executor.memory="6g", 
                  spark.storage.memoryFraction = "0.2",
                  spark.serializer="org.apache.spark.serializer.KryoSerializer",
                  spark.executor.extraJavaOptions="-XX:+UseCompressedOops",
                  spark.driver.memory="6g", 
                  spark.driver.maxResultSize = "6g"
                 ,spark.rdd.compress="true"
)

#master <- system("cat /root/spark-ec2/cluster-url", intern=TRUE)

# spark.context <- sparkR.init(master=master,
#                              appName = paste0("poli", Sys.time()),
#                              sparkEnvir=spark.env, sparkExecutorEnv = spark.env)
#doDebug <- TRUE
spark.context = sparkR.init("local")


data(gadarian)
#gadarian <- gadarian[1:25,]

# corpus <- textProcessor(gadarian$open.ended.response)
# prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)
# results <- stm(documents = prep$documents,
#                vocab = prep$vocab,
#                data = prep$meta, 
#                max.em.its = 20, 
#                 content = ~treatment,
#                 prevalence = ~ pid_rep + MetaID,
#                K = 4, spark.context = spark.context, 
#                spark.partitions = 120
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
                   K = 10, spark.context = spark.context, 
                   spark.partitions = 600
)
save(poliresuls, file="poliresults")



#sparkR.stop()

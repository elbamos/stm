library(pryr)
library(SparkR)
library(plyr)
library(stm)


corpus <- textProcessor(gadarian$open.ended.response)
prep <- prepDocuments(corpus$documents, corpus$vocab, gadarian)

spark.context = sparkR.init("local")

count <- 5000
example <- list()
for (i in 1:count) {
  example[[i]] <- list(index = i, list(index = i, d = matrix(rnorm(20), nrow = 2), a = i %% 5, l = rnorm(50)))
}
print(paste("list size ", object_size(example)))
# reports ~ 6 MB

example.rdd <- parallelize(spark.context, example, 10L)
# Warns that task size is 372KB -- for 2 stages
toss <- persist(example.rdd, "MEMORY_AND_DISK")
count <- count(example.rdd)
# More task size warnings
print(count)
exampletest.rdd <- mapPartitionsWithIndex(example.rdd, function(split, part) {
  out <- llply(part, .fun = function(x) {
#    x <- x[[2]]
    x$l <- rnorm(50)
    x
  })
  print(paste("output size ", object_size(out)))
  out
})
persist(exampletest.rdd, "MEMORY_AND_DISK")
sum.rdd <- mapValues(example.rdd, function (x) {
  sum(x$d)
})
grand.sum <- reduce(sum.rdd, function(x) {
  print(str(x))
  stop()
})
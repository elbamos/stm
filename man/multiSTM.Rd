\name{multiSTM}
\alias{multiSTM}
\alias{print.MultimodDiagnostic}
%- Also NEED an '\alias' for EACH other topic documented here.
\title{Analyze Stability of Local STM Mode}
\description{
This function performs a suite of tests aimed at assessing the global 
behavior of an STM model, which may have multiple modes. The function
takes in a collection of differently initialized STM fitted objects and selects
a reference model against which all others are benchmarked for stability.
The function returns an output of S3 class 'MultimodDiagnostic', with
associated plotting methods for quick inspection of the test results.
}
\usage{
multiSTM(mod.out=NULL, ref.model=NULL, align.global=FALSE, mass.threshold=1, 
    reg.formula=NULL, metadata=NULL, reg.nsims=100, 
    reg.parameter.index=2, verbose=TRUE, from.disk=FALSE)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
\item{mod.out}{
The output of a \code{selectModel()} run. This is a list of model outputs the user has to choose from, which all take the same form as the output from a STM model. Currently only works with models without content covariates.
}
\item{ref.model}{
An integer referencing the element of the list in \code{mod.out} which contains the desired reference model.  When set to the default value of \code{NULL} this chooses the model with the largest value of the approximate variational bound.
}
\item{align.global}{
A boolean parameter specifiying how to align the topics of two different STM fitted models. The alignment is performed by solving the linear sum assignment problem using the Hungarian algorithm. If \code{align.global} is set to \code{TRUE}, the Hungarian algorithm is run globally on the topic-word matrices of the two models that are being compared. The rows of the matrices are aligned such as to minimize the sum of their inner products. This results in each topic in the current runout being matched to a unique topic in the reference model. If \code{align.global} is, conversely, set to \code{FALSE}, the alignment problem is solved locally. Each topic in the current runout is matched to the one topic in the reference models that yields minimum inner product. This means that mutliple topics in the current runout can be matched to a single topic in the reference model, and does not guarantee that all the topics in the reference model will be matched.
}
\item{mass.threshold}{
A parameter specifying the portion of the probability mass of topics to be used for model analysis. The tail of the probability mass is disregarded accordingly. If \code{mass.threshold} is different from 1, both the full-mass and partial-mass analyses are carried out. 
}
\item{reg.formula}{
A formula for estimating a regression for each model in the ensemble, where the documents are the units, the outcome is the proportion of each document about a topic in an STM model, and the covariates are the document-level metadata. The formula should have an integer or a vector of numbers on the left-hand side, and an equation with covariates on the right-hand side. If the left-hand side is left blank, the regression is performed on all topics in the model. The formula is exclusively used for building calls to \code{estimateEffect()}, so see the documentation for \code{estimateEffect()} for greater detail about the regression procedure. If \code{reg.formula} is null, the covariate effect stability analysis routines are not performed. The regressions incorporate uncertainty by  using an approximation to the average covariance matrix formed using the global parameters.
}
\item{metadata}{
A dataframe where the predictor variables in \code{reg.formula} can be found. It is necessary to unclude this argument if \code{reg.formula} is specified. 
}
\item{reg.nsims}{
The number of simulated draws from the variational posterior for each call of \code{estimateEffect()}. Defaults to 100.
}
\item{reg.parameter.index}{
If \code{reg.formula} is specified, the function analyzes the stability across runs of the regression coefficient for one particular predictor variable. This argument specifies which predictor variable is to be analyzed. A value of 1 corresponds to the intercept, a value of 2 correspond to the first predictor variable in \code{reg.formula}, and so on. Support for multiple concurrent covariate effect stability analyses is forthcoming.
}
\item{verbose}{
If set to \code{TRUE}, the function will report progress.
}
\item{from.disk}{
If set to \code{TRUE}, \code{multiSTM()} will load the input models from disk rather than from RAM. This option is particularly useful for dealing with large numbers of models, and is intended to be used in conjunction with the \code{to.disk} option of \code{selectModel()}. \code{multiSTM()} inspects the current directory for RData files.
}
}
\details{
The purpose of this function is to automate and generalize the stability analysis routines for topic models that are introduced in Roberts, Margaret E., Brandon M. Stewart, and Dustin Tingley: "Navigating the Local Modes of Big Data: The Case of Topic Models" (2014). For more detailed discussion regarding the background and motivation for multimodality analysis, please refer to the original article. See also the documentation for \code{\link{plot.MultimodDiagnostic}} for help with the plotting methods associated with this function.
}
\value{
An object of 'MultimodDiagnostic' S3 class, consisting of a list with the following components:
\item{N}{The number of fitted models in the list of model outputs that was supplied to the function for the purpose of stability analysis.}
\item{K}{The number of topics in the models.}
\item{glob.max}{The index of the reference model in the list of model outputs (\code{mod.out}) that was supplied to the function. The reference model is selected as the one with the maximum bound value at convergence.}
\item{lb}{A list of the maximum bound value at convergence for each of the fitted models in the list of model outputs. The list has length N.}
\item{lmat}{A K-by-N matrix reporting the L1-distance of each topic from the corresponding one in the reference model. This is defined as: \deqn{L_{1}=\sum_{v}|\beta_{k,v}^{ref}-\beta_{k,v}^{cand}|}
Where the beta matrices are the topic-word matrices for the reference and the candidate model.}
\item{tmat}{A K-by-N matrix reporting the number of "top documents" shared by the reference model and the candidate model. The "top documents" for a given topic are defined as the 10 documents in the reference corpus with highest topical frequency.}
\item{wmat}{A K-by-N matrix reporting the number of "top words" shared by the reference model and the candidate model. The "top words" for a given topic are defined as the 10 highest-frequency words.}
\item{lmod}{A vector of length N consisting of the row sums of the \code{lmat} matrix.}
\item{tmod}{A vector of length N consisting of the row sums of the \code{tmat} matrix.}
\item{wmod}{A vector of length N consisting of the row sums of the \code{wmat} matrix.}
\item{semcoh}{Semantic coherence values for each topic within each model in the list of model outputs.}
\item{L1mat}{A K-by-N matrix reporting the limited-mass L1-distance of each topic from the corresponding one in the reference model. Similar to \code{lmat}, but computed using only the top portion of the probability mass for each topic, as specified by the \code{mass.threshol} parameter. \code{NULL} if \code{mass.treshold==1}.}
\item{L1mod}{A vector of length N consisting of the row means of the \code{L1mat} matrix.}
\item{mass.threshold}{The mass threshold argument that was supplied to the function.}
\item{cov.effects}{A list of length N containing the output of the run of \code{estimateEffect()} on each candidate model with the given regression formula. \code{NULL} if no regression formula is given.}
\item{var.matrix}{A K-by-N matrix containing the estimated variance for each of the fitted regression parameters. \code{NULL} if no regression formula is given.}
\item{confidence.ratings}{A vector of length N, where each entry specifies the proportion of regression coefficient estimates in a candidate model that fall within the .95 confidence interval for the corresponding estimate in the reference model.}
\item{align.global}{The alignment control argument that was supplied to the function.}
\item{reg.formula}{The regression formula that was supplied to the function.}
\item{reg.nsims}{The \code{reg.nsims} argument that was supplied to the function.}
\item{reg.parameter.index}{The \code{reg.parameter.index} argument that was supplied to the function.}
}
\references{
Roberts, Margaret E., Brandon M. Stewart, and Dustin Tingley. "Navigating the Local Modes of Big Data: The Case of Topic Models." Forthcoming.
}
\author{
Antonio Coppola, Harvard University
}

\seealso{
\code{\link{plot.MultimodDiagnostic}}
\code{\link{selectModel}}
\code{\link{estimateEffect}}
}

\examples{
\dontrun{

# Example using Gadarian data
temp<-textProcessor(documents=gadarian$open.ended.response, 
                    metadata=gadarian)
meta<-temp$meta
vocab<-temp$vocab
docs<-temp$documents
out <- prepDocuments(docs, vocab, meta)
docs<-out$documents
vocab<-out$vocab
meta <-out$meta
set.seed(02138)
mod.out <- selectModel(docs, vocab, K=3, 
                       prevalence=~treatment + s(pid_rep), 
                       data=meta, runs=20)

out <- multiSTM(mod.out, mass.threshold = .75, 
                reg.formula = ~ treatment,
                metadata = gadarian)
plot(out)

# Same example as above, but loading from disk
mod.out <- selectModel(docs, vocab, K=3, 
                       prevalence=~treatment + s(pid_rep), 
                       data=meta, runs=20, to.disk=T)

out <- multiSTM(from.disk=T, mass.threshold = .75, 
                reg.formula = ~ treatment,
                metadata = gadarian)

}
\dontrun{
# One more example using Poliblog data
load(url("http://goo.gl/91KbfS"))
meta <- poliblogPrevFit$settings$covariates$X
out <- multiSTM(poliblogSelect, mass.threshold=.75, 
                reg.formula= ~ ratingLiberal,
                metadata=meta)

plot(out, 1:4)
}
}
% Add one or more standard keywords, see file 'KEYWORDS' in the
% R documentation directory.
\keyword{stm}
\keyword{multimodality}

#Optimizing beta
opt.beta <- function(beta.ss, kappa, settings) {
  #if its standard lda just row normalize
  if(is.null(kappa)) return(list(beta=list(beta.ss[[1]]/rowSums(beta.ss[[1]]))))

  #If its a SAGE model use the distributed poissons
  if(settings$tau$mode=="L1") {
    out <- mnreg(beta.ss,settings)
  } else {
    out <- jeffreysKappa(beta.ss, kappa, settings) 
  }
  return(out)
}
# x is a slice of beta.ss for a single aspect
spark.settings <- function(settings) {

reduce.beta.nokappa <- function(x) {
  x/rowSums(x)
}
reduce.beta.l1 <- function(x) {
  # this function may need to get created dynamically when we create settings
  # do the mnreg step here for a single aspect
}
reduce.beta.other <- function(x) {
  # this function needs kappa and may need to get created dynamically when we create settings
  # do the jeffreysKappa step here for a single aspect
}


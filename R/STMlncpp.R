logisticnormalcpp <- function(eta, mu, siginv, beta, doc, sigmaentropy) {
  doc.ct <- doc[2,]
  Ndoc <- sum(doc.ct)
  #even at K=100, BFGS is faster than L-BFGS
  optim.out <- optim(par=eta, fn=lhoodcpp, gr=gradcpp,
                     method="BFGS", control=list(maxit=500),
                     doc_ct=doc.ct, mu=mu,
                     siginv=siginv, beta=beta)
  
  #Solve for Hessian/Phi/Bound returning the result
  hpb(optim.out$par, doc.ct=doc.ct, mu=mu,
      siginv=siginv, beta=beta, Ndoc=Ndoc,
      sigmaentropy=sigmaentropy)
}



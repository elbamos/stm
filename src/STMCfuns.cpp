// -*- mode: C++; c-indent-level: 4; c-basic-offset: 4; indent-tabs-mode: nil; -*-

// [[Rcpp::depends(RcppArmadillo)]]

#include "RcppArmadillo.h"

// [[Rcpp::depends(RcppArmadillo)]]

// [[Rcpp::export]]
double lhoodcpp(SEXP eta,
                   SEXP beta,
                   SEXP doc_ct,
                   SEXP mu,
                   SEXP siginv){
   
   Rcpp::NumericVector etav(eta); 
   arma::vec etas(etav.begin(), etav.size(), false);
   Rcpp::NumericMatrix betam(beta);
   arma::mat betas(betam.begin(), betam.nrow(), betam.ncol(), false);
   Rcpp::NumericVector doc_ctv(doc_ct);
   arma::vec doc_cts(doc_ctv.begin(), doc_ctv.size(), false);
   Rcpp::NumericVector muv(mu);
   arma::vec mus(muv.begin(), muv.size(), false);
   Rcpp::NumericMatrix siginvm(siginv);
   arma::mat siginvs(siginvm.begin(), siginvm.nrow(), siginvm.ncol(), false);
   
   arma::rowvec expeta(etas.size()+1); 
   expeta.fill(1);
   int neta = etav.size(); 
   for(int j=0; j <neta;  j++){
     expeta(j) = exp(etas(j));
   }
   double ndoc = sum(doc_cts);
   double part1 = arma::as_scalar(log(expeta*betas)*doc_cts - ndoc*log(sum(expeta)));
   arma::vec diff = etas - mus;
   double part2 = .5*arma::as_scalar(diff.t()*siginvs*diff);
   double out = part2 - part1;
   return out;
}

// [[Rcpp::export]]
arma::vec gradcpp(SEXP eta,
                   SEXP beta,
                   SEXP doc_ct,
                   SEXP mu,
                   SEXP siginv){
   
   Rcpp::NumericVector etav(eta); 
   arma::vec etas(etav.begin(), etav.size(), false);
   Rcpp::NumericMatrix betam(beta);
   arma::mat betas(betam.begin(), betam.nrow(), betam.ncol());
   Rcpp::NumericVector doc_ctv(doc_ct);
   arma::vec doc_cts(doc_ctv.begin(), doc_ctv.size(), false);
   Rcpp::NumericVector muv(mu);
   arma::vec mus(muv.begin(), muv.size(), false);
   Rcpp::NumericMatrix siginvm(siginv);
   arma::mat siginvs(siginvm.begin(), siginvm.nrow(), siginvm.ncol(), false);
   
    arma::colvec expeta(etas.size()+1); 
    expeta.fill(1);
    int neta = etas.size(); 
    for(int j=0; j <neta;  j++){
       expeta(j) = exp(etas(j));
    }
    betas.each_col() %= expeta;
    arma::vec part1 = betas*(doc_cts/arma::trans(sum(betas,0))) - (sum(doc_cts)/sum(expeta))*expeta;
    arma::vec part2 = siginvs*(etas - mus);
    part1.shed_row(neta);
    return part2-part1;
}


#include <Rcpp.h> 
#include <boost/asio/ip/address_v4.hpp>
 
using namespace Rcpp; 
using namespace boost::asio::ip;
 
// Rcpp/C++ vectorized routines
 
// [[Rcpp::export]]
NumericVector rcpp_rinet_pton (CharacterVector ip) { 
 
  int ipCt = ip.size(); // how many elements in vector
 
  NumericVector ipInt(ipCt); // allocate new numeric vector
 
  // CONVERT ALL THE THINGS!
  for (int i=0; i<ipCt; i++) {
    ipInt[i] = address_v4::from_string(ip[i]).to_ulong();
  }
 
  return(ipInt);
}
 
// [[Rcpp::export]]
CharacterVector rcpp_rinet_ntop (NumericVector ip) {
  
  int ipCt = ip.size();
 
  CharacterVector ipStr(ipCt); // allocate new character vector
  // CONVERT ALL THE THINGS!
  for (int i=0; i<ipCt; i++) {
    ipStr[i] = address_v4(ip[i]).to_string();
  }
  
  return(ipStr);
  
}
 
// orignial single-element vector routines we'll vectorize with Vectorize()
 
// [[Rcpp::export]]
unsigned long rinet_pton (CharacterVector ip) { 
  return(boost::asio::ip::address_v4::from_string(ip[0]).to_ulong());
}
 
// [[Rcpp::export]]
CharacterVector rinet_ntop (unsigned long addr) {
  return(boost::asio::ip::address_v4(addr).to_string());
}

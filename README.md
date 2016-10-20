#General

##circos_web_app/
code for [circos web app](https://circos.gadatalab.com)  
See infra below to deploy VM infrastructure on AWS

##collect/

##doc/

##infra/
To deploy infra on AWS :
- circos
- zeppelin : developpement and production environnement
- elastic search/kibana
- server usage
- wiki

##src/
 Pipeline built 
  - to compile project : sbt assembly universal:packageZipTarball  
  - to publish package : aws s3 cp ./target/universal/aldatalab-1.3.3.tgz s3://gedatalab/binaries/aldatalab_2.11-1.3.3-1.6.2.tgz  





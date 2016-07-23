###Creating clusters
  [zeppelin-dev](https://devzeppelin.gadatalab.com)  
    ./spark-ec2 -k *KeyLezoomerUs* -i ~/.ssh/*KeyLezoomerUs*.pem --pipeline-version=1.3.1 --region=us-east-1 --zone=us-east-1a --instance-type m3.xlarge --deploy-profile customers --instance-profile-name customers --master-instance-type m3.xlarge --spot-price 0.1 --master-spot-price 0.1 --deploy-env dev --zeppelin-bucket gecustomers --es-security-group elasticsearch-discovery -s 15 launch zeppelin-dev  
  [zeppelin-prod](https://zeppelin.gadatalab.com)  
    ./spark-ec2 -k *KeyLezoomerUs* -i ~/.ssh/*KeyLezoomerUs*.pem --pipeline-version=1.3.1 --region=us-east-1 --zone=us-east-1a --instance-type m3.xlarge --deploy-profile customers --instance-profile-name customers --master-instance-type m3.xlarge --spot-price 0.1 --master-spot-price 0.1 --es-security-group elasticsearch-discovery -s 5 launch zeppelin-prod  
    
  [kibana/elastic search](https://kibana.gadatalab.com)  
    
  [server usage](https://serverusage.gadatalab.com)  
    
  [proxy](https://*.gadatalab.com)  
    
  [circos web app](https://circos.gadatalab.com)  
    
  

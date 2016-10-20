
##To create a cluster, execute following commands
  - zeppelin.gadatalab.com :
    - ./spark-ec2 -k KeyLezoomerUs -i ~/.ssh/KeyLezoomerUs.pem --pipeline-version=1.3.3 --scala-version 2.11 --region=us-east-1 --zone=us-east-1b --instance-type m1.xlarge --master-instance-type m1.xlarge --spot-price 0.13 --master-spot-price 0.13 --deploy-env prod --zeppelin-bucket gecustomers --es-security-group elasticsearch-discovery --deploy-profile customers --instance-profile-name customers -s 5 launch zeppelin-prod
    - on web proxy server, associate new master IP to zeppelin upstream in /etc/nginx/conf.d/upstreams.conf file and restart nginx
  - devzeppelin.gadatalab.com :
    - ./spark-ec2 -k KeyLezoomerUs -i ~/.ssh/KeyLezoomerUs.pem --pipeline-version=1.3.3 --scala-version 2.11 --region=us-east-1 --zone=us-east-1b --instance-type m1.xlarge --master-instance-type m1.xlarge --spot-price 0.13 --master-spot-price 0.13 --deploy-env dev --zeppelin-bucket gecustomers --es-security-group elasticsearch-discovery --deploy-profile customers --instance-profile-name customers -s 10 launch zeppelin-dev
    - on web proxy server, associate new master IP to dev.zeppelin upstream in /etc/nginx/conf.d/upstreams.conf file and restart nginx
  - pipeline (no public url, use private ssh tunnel to access zeppelin (localhost:8080) :
    - ./spark-ec2 -k KeyLezoomerUs -i ~/.ssh/KeyLezoomerUs.pem --pipeline-version=1.3.3 --scala-version 2.11 --region=us-east-1 --zone=us-east-1b --instance-type m1.xlarge --master-instance-type m1.xlarge --spot-price 0.13 --master-spot-price 0.13 --deploy-env pipeline --zeppelin-bucket gezeppelin --es-security-group elasticsearch-discovery --copy-aws-credentials -s 25 launch pipeline


###Rebuild AMI

   Proxy:
   ```
   cd infra/proxy && packer build proxy.json   
   ```

   ELK:
   ```
   cd infra/elk && packer build elk.json   
   ```

   Spark:
   ```
   cd infra/spark && packer build java.json   
   ```
</br>
</br>

###Instantiate apps
  [proxy](https://*.gadatalab.com)  </br>
  
[zeppelin-dev](https://devzeppelin.gadatalab.com)  
**Create cluster** :  
  - cd spark; ./spark-ec2 -k KeyLezoomerUs -i ~/.ssh/KeyLezoomerUs.pem --pipeline-version=1.3.3 --scala-version 2.11 --region=us-east-1 --zone=us-east-1b --instance-type m1.xlarge --master-instance-type m1.xlarge --spot-price 0.13 --master-spot-price 0.13 --deploy-env dev --zeppelin-bucket gecustomers --es-security-group elasticsearch-discovery --deploy-profile customers --instance-profile-name customers -s 10 launch zeppelin-dev  
  - on web proxy server, associate new master IP to dev.zeppelin upstream in /etc/nginx/conf.d/upstreams.conf file and restart nginx  
  **Import data** : ssh root@zeppelin-master "cd pipeline/bin; ./syncHdfsS3.sh fromS3Simple"  
</br>
    
[zeppelin-prod](https://zeppelin.gadatalab.com)  
**Create cluster** :  
  - cd spark; ./spark-ec2 -k KeyLezoomerUs -i ~/.ssh/KeyLezoomerUs.pem --pipeline-version=1.3.3 --scala-version 2.11 --region=us-east-1 --zone=us-east-1b --instance-type m1.xlarge --master-instance-type m1.xlarge --spot-price 0.13 --master-spot-price 0.13 --deploy-env prod --zeppelin-bucket gecustomers --es-security-group elasticsearch-discovery --deploy-profile customers --instance-profile-name customers -s 5 launch zeppelin-prod    
  - on web proxy server, associate new master IP to zeppelin upstream in /etc/nginx/conf.d/upstreams.conf file and restart nginx  
</br>
    
[Pipeline]() (no public url, use private ssh tunnel to access zeppelin (localhost:8080) :    
**Create cluster** :  
  - cd spark; ./spark-ec2 -k KeyLezoomerUs -i ~/.ssh/KeyLezoomerUs.pem --pipeline-version=1.3.3 --scala-version 2.11 --region=us-east-1 --zone=us-east-1b --instance-type m1.xlarge --master-instance-type m1.xlarge --spot-price 0.13 --master-spot-price 0.13 --deploy-env pipeline --zeppelin-bucket gezeppelin --es-security-group elasticsearch-discovery --copy-aws-credentials -s 25 launch pipeline  
</br>
    
[kibana/elastic search](https://kibana.gadatalab.com)  
**Create cluster** :  
  - cd elk; ./elk-ec2 -k *KeyLezoomerUs* -i ~/.ssh/*KeyLezoomerUs*.pem --instance-type m3.xlarge --spot-price 0.1 --master-spot-price 0.1 --es-security-group elasticsearch-discovery -s 5 launch zeppelin-prod  
  - on web proxy server, associate new master IP to kibana upstream in /etc/nginx/conf.d/upstreams.conf file and restart nginx  
</br>
    
  [server usage](https://serverusage.gadatalab.com)  
</br>

  [circos web app](https://circos.gadatalab.com)  
    Launch last ami version named SiteMap-*, m3.xlarge, spot 0.1$, zone us-east-1a, security group SiteMapWebApp
       Associate fixed IP 52.4.60.249 to new instance  
</br>
    
  [Wiki](https://wiki.gadatalab.com)  
    ssh -i ~/.ssh/*AlWikiKey*.pem ec2-user@ec2-54-164-93-140.compute-1.amazonaws.com
    cd ALWiki; nohup forever ./node_modules/jingo/jingo -c config.yaml &
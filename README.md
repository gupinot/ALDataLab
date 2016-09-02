###Instantiate apps
  [proxy](https://*.gadatalab.com)  </br>
  
  [zeppelin-dev](https://devzeppelin.gadatalab.com)  
    **Create cluster** : ./spark-ec2 -k *KeyLezoomerUs* -i ~/.ssh/*KeyLezoomerUs*.pem --pipeline-version=1.3.1 --region=us-east-1 --zone=us-east-1a --instance-type m3.xlarge --deploy-env dev --deploy-profile customers --instance-profile-name customers --master-instance-type m3.xlarge --spot-price 0.1 --master-spot-price 0.1 --deploy-env dev --zeppelin-bucket gecustomers --es-security-group elasticsearch-discovery -s 15 launch zeppelin-dev  
    **Import data** : ssh root@zeppelin-master "cd pipeline/bin; ./syncHdfsS3.sh fromS3Simple"  </br>
    
  [zeppelin-prod](https://zeppelin.gadatalab.com)  
    **Create cluster** : ./spark-ec2 -k *KeyLezoomerUs* -i ~/.ssh/*KeyLezoomerUs*.pem --pipeline-version=1.3.1 --region=us-east-1 --zone=us-east-1a --instance-type m3.xlarge --deploy-profile customers --instance-profile-name customers --master-instance-type m3.xlarge --spot-price 0.1 --master-spot-price 0.1 --es-security-group elasticsearch-discovery -s 5 launch zeppelin-prod  
    </br>
    
  [kibana/elastic search](https://kibana.gadatalab.com)  
    
  [server usage](https://serverusage.gadatalab.com)  
    
  [circos web app](https://circos.gadatalab.com)  
    Launch ami-e8be34ff, m3.xlarge, spot 0.1$, zone us-east-1a, security group SiteMapWebApp  
    Associate fixed IP 52.4.60.249 to new instance  </br>
    
  [Wiki](https://wiki.gadatalab.com)  
    ssh -i ~/.ssh/*AlWikiKey*.pem ec2-user@ec2-54-164-93-140.compute-1.amazonaws.com
    cd ALWiki; nohup forever ./node_modules/jingo/jingo -c config.yaml &

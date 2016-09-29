server {
    listen 443 ssl;
    server_name s3.gadatalab.com;

    add_header Strict-Transport-Security max-age=31536000;

    #aws_access_key AKIAIVELPAADXQE7QMFQ;
    #aws_key_scope 20160718/us-east-1/s3/aws4_request;
    #aws_signing_key DH/11FrMP3MXTuPquG6KhnEwL7OnbB5/2MBOT3Tf0Po=;
    aws_s3_bucket gecustomers;

    location / {
        include includes/jwt-config.conf;
        include includes/jwt-acl.conf;
        include includes/jwt-auth.conf;

    	aws_sign;
        rewrite /(.*) /document/$1 break;
        proxy_pass http://gecustomers.s3.amazonaws.com;
    }
}

server {
    listen 443 ssl;
    server_name s3.gadatalab.com;

    add_header Strict-Transport-Security max-age=31536000;

    aws_s3_bucket gecustomers;

    location / {
        if ($ssl_client_verify != SUCCESS) {
            return 401;
        }

        if ($ssl_client_s_dn_cn !~ "^(.*@aptiwan.com|.*@lezoomer.com|geuser|Guillaume PINOT)$") {
            return 403;
        }

	aws_sign;
        rewrite /(.*) /document/$1 break;
        proxy_pass http://gecustomers.s3.amazonaws.com;
    }
}

server {
    listen 443 ssl;
    server_name circos.gadatalab.com;

    add_header Strict-Transport-Security max-age=31536000;

    location / {
        if ($ssl_client_verify != SUCCESS) {
            return 401;
        }

        if ($ssl_client_s_dn_cn !~ "^(.*@aptiwan.com|.*@lezoomer.com|geuser|Guillaume PINOT)$") {
            return 403;
        }

        proxy_pass http://circos;
	proxy_read_timeout 600;
    }
}

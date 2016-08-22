server {
    listen 443 ssl;
    server_name kibana.gadatalab.com;

    root /usr/share/nginx/html;

    add_header Strict-Transport-Security max-age=31536000;

    location / {
        if ($ssl_client_verify != SUCCESS) {
            return 401;
        }

        if ($ssl_client_s_dn_cn !~ "^(.*@aptiwan.com|.*@lezoomer.com|geuser|Guillaume PINOT)$") {
            return 403;
        }

        proxy_pass http://kibana;
    }
}

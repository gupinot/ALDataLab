server {
    listen 443 ssl;
    server_name devzeppelin.gadatalab.com;

    root /usr/share/nginx/html;

    add_header Strict-Transport-Security max-age=31536000;

    location / {
        if ($ssl_client_verify != SUCCESS) {
            return 401;
        }

        if ($ssl_client_s_dn_cn !~ "^(.*@aptiwan.com|.*@lezoomer.com|geuser|Guillaume PINOT)$") {
            return 403;
        }

        proxy_pass http://dev.zeppelin;
    }

    location /ws {
        proxy_pass http://dev.zeppelin;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }
}

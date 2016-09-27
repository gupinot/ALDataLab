server {
    listen 443 ssl;
    server_name devzeppelin.gadatalab.com;

    root /usr/share/nginx/html;

    add_header Strict-Transport-Security max-age=31536000;

    location = /ws {
        proxy_pass http://dev.zeppelin;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    location / {
        include includes/jwt-config.conf;
        include includes/jwt-auth.conf;
        include includes/jwt-acl.conf;

        proxy_pass http://dev.zeppelin;
    }

}

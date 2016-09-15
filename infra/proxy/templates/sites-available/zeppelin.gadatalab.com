server {
    listen 443 ssl;
    server_name zeppelin.gadatalab.com;

    root /usr/share/nginx/html;

    add_header Strict-Transport-Security max-age=31536000;

    location = /ws {
        proxy_pass http://zeppelin;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
    }

    location / {
        include includes/acl.conf;
        proxy_pass http://zeppelin;
    }
}

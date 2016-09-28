server {
    listen 443 ssl;
    server_name wiki.gadatalab.com;

    root /usr/share/nginx/html;

    add_header Strict-Transport-Security max-age=31536000;

    location / {
        include includes/jwt-config.conf;
        include includes/jwt-acl.conf;
        include includes/jwt-auth.conf;

        proxy_pass http://wiki;
    }
}

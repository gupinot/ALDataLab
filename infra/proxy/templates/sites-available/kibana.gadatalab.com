server {
    listen 443 ssl;
    server_name kibana.gadatalab.com;

    root /usr/share/nginx/html;

    add_header Strict-Transport-Security max-age=31536000;

    location / {
        include includes/acl.conf;

        proxy_pass http://kibana;
    }
}

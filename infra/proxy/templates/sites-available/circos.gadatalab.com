server {
    listen 443 ssl;
    server_name circos.gadatalab.com;

    add_header Strict-Transport-Security max-age=31536000;

    location / {
        include includes/acl.conf;

        proxy_pass http://circos;
	proxy_read_timeout 600;
    }
}

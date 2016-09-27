server {
    listen 443 ssl;
    server_name auth.gadatalab.com;

    add_header Strict-Transport-Security max-age=31536000;

    ssl_verify_client optional;
    auth_basic "Datalab Auth";
    auth_basic_user_file users;

    location / {
        include includes/jwt-config.conf;
        content_by_lua_file /etc/nginx/lua/set_jwt.lua;
    }
}

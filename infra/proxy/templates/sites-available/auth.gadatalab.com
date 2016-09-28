server {
    listen 443 ssl;
    server_name auth.gadatalab.com;

    add_header Strict-Transport-Security max-age=31536000;

    ssl_verify_client optional;

    location / {
        if ($ssl_client_verify != SUCCESS) {
            return 302 https://${server_name}/basic$is_args$args;
        }
        include includes/jwt-config.conf;
        content_by_lua_file /etc/nginx/lua/set_jwt.lua;
    }

    location /basic {
       auth_basic "Datalab Auth";
       auth_basic_user_file users;
       include includes/jwt-config.conf;
       content_by_lua_file /etc/nginx/lua/set_jwt.lua;
    }
}

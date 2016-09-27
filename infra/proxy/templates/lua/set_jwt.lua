local jwt = require "resty.jwt"

-- get current identity, SSL is priority
local login = ngx.var.ssl_login;
if not login or login == "" then
    login = ngx.var.remote_user;
end

-- user is neither auth by SSL nor by basic auth, reject
if not login or login == "" then
    ngx.status = ngx.HTTP_UNAUTHORIZED
    ngx.exit(ngx.HTTP_OK)
end

-- build the token
local jwt_token = jwt:sign(
    ngx.var.jwt_secret,
    {
        header={typ="JWT", alg="HS256"},
        payload={sub=login}
    }
)

-- build and set the cookie
local expires = 3600 * 24 * 30 -- 30 days
local cookie_expire = ngx.cookie_time(ngx.time() +expires)
ngx.header["Set-Cookie"] = "jwt=" .. jwt_token .. "; path=/; HttpOnly; Secure; domain=.gadatalab.com; Expires=" .. cookie_expire;

-- redirect if next url is set
local next_url = ngx.var.arg_next
if jwt_token then
    if next_url then
        ngx.redirect(next_url)
    else
        ngx.say("Hello " .. login)
        ngx.exit(ngx.HTTP_OK)
    end
else
    ngx.say("No token")
    ngx.exit(ngx.HTTP_OK)
end
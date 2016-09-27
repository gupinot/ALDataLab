local jwt = require "resty.jwt"
local validators = require "resty.jwt-validators"
local jwt_token = ngx.var.cookie_jwt
local ssl_login = ngx.var.ssl_login
local this_url = ngx.escape_uri(ngx.var.scheme .. "://" .. ngx.var.http_host .. ngx.var.request_uri)
local next_url= ngx.var.jwt_auth_url .. "?next=" .. this_url

local claim_spec = {
    sub = validators.required()
}

if ssl_login and not ssl_login == "" then
    ngx.var.fakeAuth = "Basic " .. ngx.encode_base64(ssl_login .. ":password");
    ngx.exit(ngx.OK)
end

if jwt_token then
    local jwt_obj = jwt:verify(ngx.var.jwt_secret, jwt_token, claim_spec)
    ngx.var.ssl_login=cjson.encode(jwt_obj);

    if jwt_obj["verified"] then
        ngx.var.ssl_login=jwt_obj["payload"]["sub"];
        ngx.var.fakeAuth = "Basic " .. ngx.encode_base64(ngx.var.ssl_login .. ":password");
        ngx.exit(ngx.OK)
    else
        ngx.redirect(next_url)
    end
else
    ngx.redirect(next_url)
end
local jwt = require "resty.jwt"
local validators = require "resty.jwt-validators"
local jwt_token = ngx.var.cookie_jwt
local ssl_login = ngx.var.ssl_login

local claim_spec = {
    sub = validators.required()
}

if ssl_login and not ssl_login == "" then
    ngx.exit(ngx.OK)
end

if jwt_token then
    local jwt_obj = jwt:verify(ngx.var.jwt_secret, jwt_token, claim_spec)

    if jwt_obj["verified"] then
        ngx.var.ssl_login=jwt_obj["payload"]["sub"];
    end
end

ngx.exit(ngx.OK)

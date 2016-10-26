local login = ngx.var.ssl_login
local pattern = ngx.var.acl_pattern
local this_url = ngx.escape_uri(ngx.var.scheme .. "://" .. ngx.var.http_host .. ngx.var.request_uri)
local next_url= ngx.var.jwt_auth_url .. "?next=" .. this_url

if login and not login == "" then
    local m,err = ngx.re.match(login,"^" .. pattern .. "$")
    if m then
        ngx.var.fakeAuth = "Basic " .. ngx.encode_base64(login .. ":password");
        return ngx.OK
    else
        return ngx.HTTP_FORBIDDEN
    end
else
    ngx.redirect(next_url)
end
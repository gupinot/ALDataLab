local re = ngx.re

local _M = {}

function _M.validate(subject,pattern)
    local m,err = re.match(subject,pattern)
    if m then
        return ngx.OK
    else
        return ngx.HTTP_FORBIDDEN
    end
end

return _M
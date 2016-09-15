FROM ubuntu:16.04
MAINTAINER raphael.luta@aptiwan.com

ADD VERSION /
RUN apt-get update -y && apt-get install -y dpkg-dev git
RUN cd /opt && git clone https://github.com/anomalizer/ngx_aws_auth
RUN cd /opt && git clone https://github.com/simpl/ngx_devel_kit
RUN cd /opt && git clone https://github.com/openresty/set-misc-nginx-module
RUN cd /opt && git clone https://github.com/openresty/lua-nginx-module
RUN mkdir -p /opt/nginx
WORKDIR /opt/nginx
RUN apt-get source nginx
RUN apt-get build-dep -y nginx 
ADD rules.patch /opt/nginx/
RUN cd /opt/nginx/nginx-* && patch debian/rules ../rules.patch
VOLUME /mnt
CMD cd /opt/nginx/nginx-* && dpkg-buildpackage -b && rm ../*dbg*deb && mv ../nginx-*.deb /mnt/

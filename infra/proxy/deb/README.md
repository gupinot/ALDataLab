# Deb packages

Nginx packages built using the Dockerfile image should be stored here

To regenerate:

```
cd ..
docker build -t nginx-builder .
docker run -it -v `pwd`deb:/mnt nginx-builder  
```
## 第三个版本
## Redis container
>docker run -d --name rkredis --restart always -p 6379:6379 -v /opt/redis/data:/data redis:latest redis-server --appendonly yes --requirepass "rkwork"

>
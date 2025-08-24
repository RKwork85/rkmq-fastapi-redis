## Fastapi + Redis + Worker 实现消息队列


## Quick start

### 一、docker安装redis 

```
# 1. 拉取最新的 Redis 镜像
docker pull redis:latest

# 2. 运行 Redis 容器
#   -d           后台运行
#   --name       容器名称
#   --restart    always 表示开机自启
#   -p 6379:6379 端口映射
#   -v           数据持久化到本地
#   --requirepass 设置密码
docker run -d \
  --name rkredis \
  --restart always \
  -p 6379:6379 \
  -v /opt/redis/data:/data \
  redis:latest \
  redis-server --appendonly yes --requirepass "rkwork"

# 3. 验证是否启动成功
docker logs rkredis

# 4. 测试连接（密码是 rkwork）
docker exec -it rkredis redis-cli -a rkwork

```
### 二、安装依赖

> pip install -r requirements.txt

### 三、启动服务
>python app.py
or
>uvicorn app:app --host 127.0.0.1 --port 8000 --reload

>python worker.py

>python client_test.py

---

--> 打印信息

```
============================================================
开始 FastAPI + Redis + RQ 测试
============================================================

1. 健康检查:
   状态: healthy
   Redis: connected
   队列大小: 0

2. 提交任务:
   ✅ 任务 1: data_analysis -> rkwork001_1755268808
   ✅ 任务 2: data_analysis -> rkwork002_1755268810

```

--> 生成文件目录截图

![alt text](./assests/document/image.png)


### versions

- v1 最外层的app.py文件： 基本的消息队列demo

- v2 增加了任务的分发机制

- v3 完善了v2 个人项目 定制化


### assets

v1~v2: 通用
>解压到项目根目录即可

v3： 私用
> 不用下载

# main.py - FastAPI 应用主文件
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from redis import Redis
from rq import Queue, Worker
from rq.job import Job
import time
import random
from typing import Optional
from datetime import datetime
from task import run_video_combinator
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 初始化 FastAPI 应用
app = FastAPI(
    title="消息队列集成示例",
    description="使用 Redis + RQ 处理耗时任务",
    version="1.1.0"
)

# Redis 连接配置
redis_client = Redis(
    host='localhost', 
    port=6379, 
    db=0, 
    password='rkwork', 
    decode_responses=False  # 保持二进制格式避免序列化问题
)
task_queue = Queue('default', connection=redis_client)

# 请求模型
class TaskRequest(BaseModel):
    task_id: str
    task_type: str
    video_type: str
    data: dict
    priority: str = "normal"


class TaskResponse(BaseModel):
    job_id: str
    status: str 
    message: str

class TaskStatusResponse(BaseModel):
    job_id: str
    status: str
    result: Optional[dict] = None
    error: Optional[str] = None
    created_at: Optional[str] = None
    finished_at: Optional[str] = None

# 任务函数模块化（避免序列化问题）
def run_task(task_info: dict):
    """任务分发器"""
    task_type = task_info["task_type"]
    data = task_info["data"]
    
    task_handlers = {
        "data_analysis": process_data_task,
        "file_processing": process_data_task,
        "model_training": process_data_task,
        "notification": send_notification_task,
        "report": generate_report_task
    }
    
    handler = task_handlers.get(task_type, process_data_task)
    return handler(task_type, data)

# 模拟耗时任务函数
def process_data_task(task_type: str, data: dict):
    """模拟数据处理任务"""
    logger.info(f"开始处理数据任务: {task_type}, 数据: {data}")
    
    # 模拟不同类型的耗时操作
    if task_type == "data_analysis":
        time.sleep(random.randint(1, 3))  # 缩短时间用于测试
        result = {
            "analysis_result": "数据分析完成",
            "processed_records": random.randint(1000, 5000),
            "insights": ["趋势上升", "异常值检测", "相关性分析"]
        }
    elif task_type == "file_processing":
        time.sleep(random.randint(20, 30))
        result = {
            "processing_result": "文件处理完成",
            "processed_files": data.get("file_count", 1),
            "output_format": data.get("format", "json")
        }
    elif task_type == "model_training":
        time.sleep(random.randint(3, 5))
        result = {
            "training_result": "模型训练完成",
            "model_accuracy": round(random.uniform(0.85, 0.98), 4),
            "epochs": data.get("epochs", 100),
            "loss": round(random.uniform(0.01, 0.1), 4)
        }
    else:
        time.sleep(random.randint(1, 2))
        result = {
            "result": "任务处理完成",
            "task_type": task_type,
            "processed_data": data
        }
    
    logger.info(f"数据任务完成: {task_type}")
    return result

# 修改任务函数签名，统一使用 task_type 和 data
def send_notification_task(task_type: str, data: dict):
    """发送通知任务"""
    logger.info(f"发送通知任务: {data}")
    time.sleep(random.randint(1, 2))
    
    if "recipient" not in data:
        raise ValueError("缺少收件人信息")
    
    return {
        "notification_sent": True,
        "recipient": data.get("recipient"),
        "message": data.get("message", "默认通知"),
        "sent_at": datetime.now().isoformat()
    }

def generate_report_task(task_type: str, data: dict):
    """生成报告任务"""
    logger.info(f"生成报告任务: {data}")
    time.sleep(random.randint(3, 5))
    
    return {
        "report_generated": True,
        "report_type": data.get("report_type", "summary"),
        "file_path": f"/reports/report_{int(time.time())}.pdf",
        "pages": random.randint(10, 50),
        "parameters": data.get("parameters", {})
    }

# API 端点
@app.get("/", summary="根端点", description="返回 API 的基本信息和可用的接口列表。")
async def root():
    """根端点"""
    return {
        "message": "FastAPI + Redis + RQ 消息队列示例",
        "version": app.version,
        "endpoints": {
            "submit_task": "POST /tasks",
            "get_task_status": "GET /tasks/{job_id}",
            "list_tasks": "GET /tasks",
            "queue_info": "GET /queue/info",
            "cancel_task": "DELETE /tasks/{job_id}",
            "clear_queue": "POST /queue/clear",
            "health_check": "GET /health"
        }
    }

@app.post("/tasks", response_model=TaskResponse, summary="提交任务", description="提交一个任务到消息队列进行异步处理。")
async def submit_task(request: TaskRequest):
    """提交异步任务"""
    try:
        # 创建任务信息字典
        
        task_info = {
            "task_id": request.task_id,
            "task_type": request.task_type,          # 任务类型
            "video_type": request.video_type,        # 视频类型
            "data": request.data,                    # 原始任务数据，可包含业务参数
            "priority": "normal",                    # 优先级，默认 normal
        }
        
        job = task_queue.enqueue(
            run_video_combinator,
            task_info,
            job_timeout='300s',  # 5分钟超时
            job_id=f"{task_info['task_id']}_{int(time.time())}",
        )  
        
        logger.info(f"任务已提交: {job.id} ({request.task_type})")
        return TaskResponse(
            job_id=job.id,
            status="queued",
            message=f"任务已提交到队列，任务ID: {job.id}"
        )
        
    except Exception as e:
        logger.error(f"提交任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"提交任务失败: {str(e)}")

@app.get("/tasks/{job_id}", response_model=TaskStatusResponse, summary="获取任务状态", description="查询指定任务的状态及其执行结果。")
async def get_task_status(job_id: str):
    """获取任务状态"""
    try:
        job = Job.fetch(job_id, connection=redis_client)
        
        response = TaskStatusResponse(
            job_id=job_id,
            status=job.get_status(),
            created_at=job.enqueued_at.isoformat() if job.enqueued_at else None
        )
        
        if job.is_finished:
            response.result = job.result
            response.finished_at = job.ended_at.isoformat() if job.ended_at else None
        elif job.is_failed:
            response.error = job.exc_info.strip() if job.exc_info else "任务执行失败"
            response.finished_at = job.ended_at.isoformat() if job.ended_at else None
        
        return response
        
    except Exception as e:
        logger.error(f"获取任务状态失败: {job_id} - {str(e)}")
        raise HTTPException(status_code=404, detail=f"未找到任务 {job_id}")

@app.get("/tasks", summary="列出所有任务", description="获取队列中所有任务的详细信息。")
async def list_tasks():
    """列出所有任务"""
    try:
        # 获取不同状态的任务
        queued_jobs = [job.id for job in task_queue.get_jobs()]
        started_jobs = list(task_queue.started_job_registry.get_job_ids())
        failed_jobs = list(task_queue.failed_job_registry.get_job_ids())
        finished_jobs = list(task_queue.finished_job_registry.get_job_ids())
        
        all_jobs = []
        all_job_ids = set(queued_jobs + started_jobs + failed_jobs + finished_jobs)
        
        for job_id in all_job_ids:
            try:
                job = Job.fetch(job_id, connection=redis_client)
                all_jobs.append({
                    "job_id": job.id,
                    "status": job.get_status(),
                    "task_type": job.args[0]["task_type"] if job.args else "unknown",
                    "created_at": job.enqueued_at.isoformat() if job.enqueued_at else None,
                    "finished_at": job.ended_at.isoformat() if job.ended_at else None
                })
            except Exception as job_err:
                logger.warning(f"无法获取任务信息 {job_id}: {str(job_err)}")
                continue
        
        return {
            "total_tasks": len(all_jobs),
            "tasks": all_jobs
        }
        
    except Exception as e:
        logger.error(f"获取任务列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取任务列表失败: {str(e)}")

@app.get("/queue/info", summary="获取队列信息", description="获取当前 Redis 消息队列的状态和统计信息。")
async def get_queue_info():
    """获取队列信息"""
    try:
        return {
            "queue_name": task_queue.name,
            "queued_jobs": len(task_queue),
            "started_jobs": len(task_queue.started_job_registry),
            "failed_jobs": len(task_queue.failed_job_registry),
            "finished_jobs": len(task_queue.finished_job_registry),
            "scheduled_jobs": len(task_queue.scheduled_job_registry),
            "workers": len(Worker.all(connection=redis_client))
        }
        
    except Exception as e:
        logger.error(f"获取队列信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取队列信息失败: {str(e)}")

@app.delete("/tasks/{job_id}", summary="取消任务", description="取消一个正在队列中排队或正在处理的任务。")
async def cancel_task(job_id: str):
    """取消任务"""
    try:
        job = Job.fetch(job_id, connection=redis_client)
        
        if job.get_status() in ['queued', 'started']:
            job.cancel()
            logger.info(f"任务已取消: {job_id}")
            return {"message": f"任务 {job_id} 已取消"}
        else:
            return {"message": f"任务 {job_id} 无法取消，当前状态: {job.get_status()}"}
            
    except Exception as e:
        logger.error(f"取消任务失败: {job_id} - {str(e)}")
        raise HTTPException(status_code=404, detail=f"取消任务失败: {str(e)}")

@app.post("/queue/clear", summary="清空队列", description="清空所有待处理的任务，谨慎使用。")
async def clear_queue():
    """清空队列"""
    try:
        # 清空所有队列状态
        task_queue.empty()
        task_queue.failed_job_registry.empty()
        task_queue.finished_job_registry.empty()
        
        logger.warning("队列已清空")
        return {"message": "队列已清空"}
        
    except Exception as e:
        logger.error(f"清空队列失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"清空队列失败: {str(e)}")

# 健康检查端点
@app.get("/health", summary="健康检查", description="检查 FastAPI 应用和 Redis 服务的健康状态。")
async def health_check():
    """健康检查"""
    try:
        # 检查 Redis 连接
        redis_client.ping()
        redis_status = "connected"
        queue_size = len(task_queue)
    except Exception as e:
        logger.error(f"Redis连接失败: {str(e)}")
        redis_status = "disconnected"
        queue_size = 0
    
    return {
        "status": "healthy" if redis_status == "connected" else "unhealthy",
        "redis": redis_status,
        "queue_size": queue_size,
        "timestamp": datetime.now().isoformat()
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app="app:app", host="0.0.0.0", port=8000, reload=True)

# {
#   "input_folder": "catoutput/baby_cat/",
#   "output_folder": "./rkwork/output_videos",
#   "ttf_path": "fonts/汉仪晓波花月圆W.ttf",
#   "min_duration": 20,
#   "max_duration": 45,
#   "max_clips_per_video": 15,
#   "max_videos": 5,
#   "max_usage": 15
# }
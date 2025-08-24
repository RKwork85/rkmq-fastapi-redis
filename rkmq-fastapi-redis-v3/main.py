'''
* @Description: 视频处理任务队列 - 支持多种任务类型
* @Author: rkwork
* @Date: 2025-08-16
* @LastEditTime: 2025-08-16
'''
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware

from redis import Redis
from rq import Queue, Worker
from rq.job import Job
import time
import random
from typing import Optional
from datetime import datetime
import logging

# 导入你的任务执行模块
from split_mixer import run_video_processor  # 假设这是你的split_mixer.py中的主要函数
from mixer import run_video_combinator             # 假设这是你的mixer.py中的主要函数

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



# 初始化 FastAPI 应用
app = FastAPI(
    title="视频处理任务队列",
    description="使用 Redis + RQ 处理视频任务 - 支持多种任务类型",
    version="1.2.0"
)

# CORS配置
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# Redis 连接配置
redis_client = Redis(
    host='localhost', 
    port=6379, 
    db=0, 
    password='rkwork', 
    decode_responses=False
)
task_queue = Queue('rk_default', connection=redis_client)

# 请求模型
class TaskRequest(BaseModel):
    task_id: str
    task_type: str  # 支持: "split_mixer", "mixer", "video_processor", "data_analysis" 等
    video_type: str = "default"
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

# 核心任务分发器
def run_task_dispatcher(task_info: dict):
    """
    统一任务分发器 - 根据task_type调用不同的处理函数
    """
    task_type = task_info.get("task_type")
    task_id = task_info.get("task_id", "unknown")
    
    logger.info(f"开始执行任务: {task_id}, 类型: {task_type}")
    
    # 任务类型映射表
    task_handlers = {
        "split_mixer": handle_split_mixer_task,
        "mixer": handle_mixer_task, 
        "video_processor": handle_video_processor_task,
        "data_analysis": handle_data_analysis_task,
        "file_processing": handle_file_processing_task,
        "model_training": handle_model_training_task,
        "notification": handle_notification_task,
        "report": handle_report_task
    }
    
    # 获取对应的处理器
    handler = task_handlers.get(task_type)
    if not handler:
        raise ValueError(f"不支持的任务类型: {task_type}")
    
    try:
        # 执行任务并返回结果
        result = handler(task_info)
        logger.info(f"任务完成: {task_id}")
        return {
            "task_id": task_id,
            "task_type": task_type,
            "status": "completed",
            "result": result,
            "completed_at": datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"任务执行失败: {task_id} - {str(e)}")
        raise

# 具体任务处理函数
def handle_split_mixer_task(task_info: dict):
    """处理 split_mixer 任务"""
    data = task_info.get("data", {})
    
    # 验证必要参数
    required_params = ["input_folder", "output_folder"]
    for param in required_params:
        if param not in data:
            raise ValueError(f"split_mixer任务缺少必要参数: {param}")
    
    # 调用 split_mixer.py 中的处理函数
    data = {
    "input_folder": data.get("input_folder"),
    "output_folder": data.get("output_folder"),
    "ttf_path": data.get("ttf_path", "assets/fonts/default.ttf"),
    "min_duration": data.get("min_duration", 10),
    "max_duration": data.get("max_duration", 30),
    "max_clips_per_video": data.get("max_clips_per_video", 15),
    "max_videos": data.get("max_videos", 1),
    "max_usage": data.get("max_usage", 15)
}
    result = run_video_processor(data)
    
    return {
        "task_type": "split_mixer",
        "processing_result": "split_mixer 处理完成",
        "details": result
    }

def handle_mixer_task(task_info: dict):
    """处理 mixer 任务"""
    data = task_info.get("data", {})
    
    # 验证必要参数
    required_params = ["input_folder", "output_folder"]
    for param in required_params:
        if param not in data:
            raise ValueError(f"mix任务缺少必要参数: {param}")
    
    # 调用 mixer.py 中的处理函数
    data = data = {
    "input_folder": data.get("input_folder"),
    "output_folder": data.get("output_folder"),
    "ttf_path": data.get("ttf_path", "assets/fonts/default.ttf"),
    "min_duration": data.get("min_duration", 10),
    "max_duration": data.get("max_duration", 30),
    "max_clips_per_video": data.get("max_clips_per_video", 20),
    "max_videos": data.get("max_videos", 1),
    "max_usage": data.get("max_usage", 10)
}
    # 创建目录（如果不存在）
    os.makedirs(data.get("output_folder"), exist_ok=True)

    result = run_video_combinator(data)
    
    return {
        "task_type": "mixer",
        "processing_result": "Mix 处理完成",
        "details": result
    }

def handle_video_processor_task(task_info: dict):
    """处理原有的 video_processor 任务"""
    # 保持原有逻辑
    return run_video_processor(task_info)

def handle_data_analysis_task(task_info: dict):
    """处理数据分析任务"""
    data = task_info.get("data", {})
    time.sleep(random.randint(1, 3))
    
    return {
        "analysis_result": "数据分析完成",
        "processed_records": random.randint(1000, 5000),
        "insights": ["趋势上升", "异常值检测", "相关性分析"],
        "parameters": data
    }

def handle_file_processing_task(task_info: dict):
    """处理文件处理任务"""
    data = task_info.get("data", {})
    time.sleep(random.randint(20, 30))
    
    return {
        "processing_result": "文件处理完成",
        "processed_files": data.get("file_count", 1),
        "output_format": data.get("format", "json")
    }

def handle_model_training_task(task_info: dict):
    """处理模型训练任务"""
    data = task_info.get("data", {})
    time.sleep(random.randint(3, 5))
    
    return {
        "training_result": "模型训练完成",
        "model_accuracy": round(random.uniform(0.85, 0.98), 4),
        "epochs": data.get("epochs", 100),
        "loss": round(random.uniform(0.01, 0.1), 4)
    }

def handle_notification_task(task_info: dict):
    """处理通知任务"""
    data = task_info.get("data", {})
    time.sleep(random.randint(1, 2))
    
    if "recipient" not in data:
        raise ValueError("缺少收件人信息")
    
    return {
        "notification_sent": True,
        "recipient": data.get("recipient"),
        "message": data.get("message", "默认通知"),
        "sent_at": datetime.now().isoformat()
    }

def handle_report_task(task_info: dict):
    """处理报告生成任务"""
    data = task_info.get("data", {})
    time.sleep(random.randint(3, 5))
    
    return {
        "report_generated": True,
        "report_type": data.get("report_type", "summary"),
        "file_path": f"/reports/report_{int(time.time())}.pdf",
        "pages": random.randint(10, 50),
        "parameters": data
    }

# API 端点
@app.get("/", summary="根端点")
async def root():
    """根端点"""
    return {
        "message": "视频处理任务队列 - 支持多种任务类型",
        "version": app.version,
        "supported_task_types": [
            "split_mixer", "mixer", "video_processor", 
            "data_analysis", "file_processing", 
            "model_training", "notification", "report"
        ],
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

@app.post("/tasks", response_model=TaskResponse, summary="提交任务")
async def submit_task(request: TaskRequest):
    """提交异步任务"""
    try:
        # 验证任务类型
        supported_types = [
            "split_mixer", "mixer", "video_processor", 
            "data_analysis", "file_processing", 
            "model_training", "notification", "report"
        ]
        
        if request.task_type not in supported_types:
            raise HTTPException(
                status_code=400, 
                detail=f"不支持的任务类型: {request.task_type}. 支持的类型: {supported_types}"
            )
        
        # 创建任务信息字典
        task_info = {
            "task_id": request.task_id,
            "task_type": request.task_type,
            "video_type": request.video_type,
            "data": request.data,
            "priority": request.priority,
            "submitted_at": datetime.now().isoformat()
        }
        
        # 根据任务类型设置不同的超时时间
        timeout_map = {
            "split_mixer": "600s",        # 10分钟
            "mixer": "600s",             # 10分钟
            "video_processor": "300s", # 5分钟
            "file_processing": "1800s", # 30分钟
            "model_training": "3600s", # 1小时
            "data_analysis": "300s",   # 5分钟
            "notification": "60s",     # 1分钟
            "report": "600s"           # 10分钟
        }
        
        job = task_queue.enqueue(
            run_task_dispatcher,  # 使用统一的分发器
            task_info,
            job_timeout=timeout_map.get(request.task_type, "300s"),
            job_id=f"{request.task_id}_{int(time.time())}",
        )
        
        logger.info(f"任务已提交: {job.id} ({request.task_type})")
        return TaskResponse(
            job_id=job.id,
            status="queued",
            message=f"任务已提交到队列，任务ID: {job.id}, 类型: {request.task_type}"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"提交任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"提交任务失败: {str(e)}")

# 其他端点保持不变...
@app.get("/tasks/{job_id}", response_model=TaskStatusResponse, summary="获取任务状态")
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

@app.get("/tasks", summary="列出所有任务")
async def list_tasks():
    """列出所有任务"""
    try:
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

@app.get("/queue/info", summary="获取队列信息")
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

@app.delete("/tasks/{job_id}", summary="取消任务")
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

@app.post("/queue/clear", summary="清空队列")
async def clear_queue():
    """清空队列"""
    try:
        task_queue.empty()
        task_queue.failed_job_registry.empty()
        task_queue.finished_job_registry.empty()
        
        logger.warning("队列已清空")
        return {"message": "队列已清空"}
        
    except Exception as e:
        logger.error(f"清空队列失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"清空队列失败: {str(e)}")

@app.get("/health", summary="健康检查")
async def health_check():
    """健康检查"""
    try:
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
    uvicorn.run(app="main:app", host="0.0.0.0", port=8889, reload=True)
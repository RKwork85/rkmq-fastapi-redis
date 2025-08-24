import requests
import time
import json

BASE_URL = "http://localhost:8000"

def test_scenario():
    """完整的API测试场景"""
    print("=" * 60)
    print("开始 FastAPI + Redis + RQ 测试")
    print("=" * 60)
    
    # 1. 检查健康状态
    try:
        print("\n1. 健康检查:")
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        health = response.json()
        print(f"   状态: {health.get('status', 'unknown')}")
        print(f"   Redis: {health.get('redis', 'unknown')}")
        print(f"   队列大小: {health.get('queue_size', 0)}")
        
        if health.get('status') != 'healthy':
            print("   ⚠️ 警告: 服务健康状态异常")
            
    except requests.exceptions.RequestException as e:
        print(f"   ❌ 健康检查失败: {str(e)}")
        print("   请确保FastAPI服务正在运行 (python main.py)")
        return
    
    # 2. 提交多种任务
    print("\n2. 提交任务:")
    tasks = [
            {
                "task_id": "rkwork001", 
                "task_type": "data_analysis", 
                "video_type": "视频baby混剪",
                "data": {
                "input_folder": "assests/videos", 
                "output_folder": "./rkwork/output_videos",
                "ttf_path": "assests/fonts/汉仪晓波花月圆W.ttf",
                "min_duration": 20,
                "max_duration": 45,
                "max_clips_per_video": 15,
                "max_videos": 5,
                "max_usage": 15
            },
            },
            {
                "task_id": "rkwork002", 
                "task_type": "data_analysis", 
                "video_type": "视频cat混剪",
                "data": {
                "input_folder": "assests/videos", 
                "output_folder": "./muzi/output_videos",
                "ttf_path": "assests/fonts/汉仪晓波花月圆W.ttf",
                "min_duration": 20,
                "max_duration": 45,
                "max_clips_per_video": 15,
                "max_videos": 5,
                "max_usage": 15
            },
            },
        ]
    job_ids = []
    for i, task in enumerate(tasks, 1):
        try:
            response = requests.post(
                f"{BASE_URL}/tasks", 
                json=task, 
                headers={"Content-Type": "application/json"},
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                job_id = result["job_id"]
                job_ids.append(job_id)
                print(f"   ✅ 任务 {i}: {task['task_type']} -> {job_id}")
            else:
                print(f"   ❌ 任务 {i} 失败: {response.status_code} - {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ 任务 {i} 提交失败: {str(e)}")
    if not job_ids:
        print("   没有成功提交的任务，测试结束")
        return
    
test_scenario()
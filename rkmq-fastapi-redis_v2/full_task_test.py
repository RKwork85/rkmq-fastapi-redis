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
        # 任务1: 完整流程（切割+混剪）
        # {
        #     "task_id": "split_mixer001", 
        #     "task_type": "split_mixer",  # 更新任务类型
        #     "video_type": "Baby视频完整处理（切割+混剪）",
        #     "data": {
        #         # === 切割相关参数 ===
        #         "input_folder": "assests/raw_videos",       # 原始视频目录
        #         "scene_threshold": 30.0,                        # 场景检测敏感度
        #         "enable_cutting": True,                         # 启用视频切割
        #         "max_concurrent_videos": 24,                     # 切割并发数
                
        #         # === 混剪相关参数 ===
        #         "output_folder": "./rkwork/split_mixer/baby_output_videos", # 最终输出目录
        #         "ttf_path": "assests/fonts/汉仪晓波花月圆W.ttf",
        #         "min_duration": 20,                             # 最小时长(秒)
        #         "max_duration": 45,                             # 最大时长(秒)
        #         "max_clips_per_video": 8,                       # 每个视频最大片段数
        #         "max_videos": 10,                                # 生成视频数量
        #         "max_usage": 10                                 # 每个素材最大使用次数
        #     }
        # },
                # 任务1: 完整流程（切割+混剪）
        {
            "task_id": "mixer001", 
            "task_type": "mixer",  # 更新任务类型
            "video_type": "Baby素材处理混剪）",
            "data": {
                # === 混剪相关参数 ===
                "input_folder": "./baby_clips", # 输入视频目录
                "output_folder": "./rkwork/mixer/baby_output_videos", # 最终输出目录
                "ttf_path": "assests/fonts/汉仪晓波花月圆W.ttf",
                "min_duration": 20,                             # 最小时长(秒)
                "max_duration": 45,                             # 最大时长(秒)
                "max_clips_per_video": 8,                       # 每个视频最大片段数
                "max_videos": 10,                                # 生成视频数量
                "max_usage": 10                                 # 每个素材最大使用次数
            }
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
                print(f"   ✅ 任务 {i}: {task['video_type']} -> {job_id}")
                print(f"      类型: {task['task_type']}")
                cutting_status = "启用" if task['data'].get('enable_cutting', False) else "禁用"
                print(f"      切割: {cutting_status}")
                print(f"      视频数: {task['data'].get('max_videos', 0)}")
            else:
                print(f"   ❌ 任务 {i} 失败: {response.status_code} - {response.text}")
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ 任务 {i} 提交失败: {str(e)}")
    
    if not job_ids:
        print("   没有成功提交的任务，测试结束")
        return
    
    # 3. 监控任务状态
    print(f"\n3. 监控任务状态 (共 {len(job_ids)} 个任务):")
    print("   开始轮询任务状态...")
    
    completed_jobs = set()
    max_wait_time = 1800  # 30分钟超时
    start_time = time.time()
    
    while len(completed_jobs) < len(job_ids) and (time.time() - start_time) < max_wait_time:
        for job_id in job_ids:
            if job_id in completed_jobs:
                continue
                
            try:
                response = requests.get(f"{BASE_URL}/tasks/{job_id}", timeout=5)
                if response.status_code == 200:
                    status_data = response.json()
                    status = status_data.get("status", "unknown")
                    
                    if status in ["finished", "failed"]:
                        completed_jobs.add(job_id)
                        result = status_data.get("result", {})
                        
                        if status == "finished":
                            print(f"   ✅ {job_id}: 完成")
                            if isinstance(result, dict):
                                if result.get("status") == "success":
                                    video_count = len(result.get("video_records", []))
                                    success_count = sum(1 for v in result.get("video_records", []) 
                                                      if v.get('渲染状态') == '成功')
                                    print(f"      视频生成: {success_count}/{video_count}")
                                    print(f"      输出目录: {result.get('output_dir', 'N/A')}")
                                    if result.get("cutting_enabled"):
                                        print(f"      切割目录: {result.get('clips_dir', 'N/A')}")
                                else:
                                    print(f"      错误: {result.get('reason', '未知错误')}")
                        else:
                            print(f"   ❌ {job_id}: 失败")
                            error_info = status_data.get("exc_info", "无错误信息")
                            print(f"      错误: {error_info}")
                    else:
                        print(f"   ⏳ {job_id}: {status}")
                        
            except requests.exceptions.RequestException as e:
                print(f"   ⚠️ {job_id}: 状态查询失败 - {str(e)}")
        
        if len(completed_jobs) < len(job_ids):
            time.sleep(10)  # 等待10秒后再次检查
    
    # 4. 最终结果汇总
    print(f"\n4. 最终结果汇总:")
    success_count = 0
    for job_id in job_ids:
        try:
            response = requests.get(f"{BASE_URL}/tasks/{job_id}", timeout=5)
            if response.status_code == 200:
                status_data = response.json()
                status = status_data.get("status", "unknown")
                if status == "finished":
                    result = status_data.get("result", {})
                    if isinstance(result, dict) and result.get("status") == "success":
                        success_count += 1
                        
        except:
            pass
    
    print(f"   总任务数: {len(job_ids)}")
    print(f"   成功任务: {success_count}")
    print(f"   完成任务: {len(completed_jobs)}")
    
    if len(completed_jobs) == len(job_ids):
        print("   🎉 所有任务已完成!")
    else:
        print(f"   ⏰ 仍有 {len(job_ids) - len(completed_jobs)} 个任务未完成")
    
    print("=" * 60)
    print("测试结束")
    print("=" * 60)

if __name__ == "__main__":
    test_scenario()
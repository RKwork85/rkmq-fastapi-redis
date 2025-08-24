# worker.py - RQ Worker 脚本
import os
import sys
from redis import Redis
from rq import Worker, Queue
import logging
import signal
import platform

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('worker.log')
    ]
)
logger = logging.getLogger('rq.worker')

# 添加当前目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# 导入任务函数 - 处理循环导入问题
try:
    from app import run_task
    logger.info("成功导入任务函数")
except ImportError as e:
    logger.error(f"导入任务函数失败: {str(e)}")
    # 定义备用任务函数
    def run_task(task_info: dict):
        """备用任务函数"""
        import time
        import random
        task_type = task_info.get("task_type", "unknown")
        data = task_info.get("data", {})
        logger.info(f"执行备用任务: {task_type} with {data}")
        time.sleep(random.randint(1, 3))
        return {"result": "备用任务完成", "task_type": task_type}

def create_redis_connection():
    """创建Redis连接"""
    try:
        redis_client = Redis(
            host='localhost', 
            port=6379, 
            db=0, 
            password='rkwork',
            decode_responses=False,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        # 测试连接
        redis_client.ping()
        logger.info("Redis连接成功")
        return redis_client
    except Exception as e:
        logger.error(f"Redis连接失败: {str(e)}")
        sys.exit(1)

def handle_worker_exception(job, exc_type, exc_value, exc_traceback):
    """自定义异常处理器"""
    logger.error(f"任务执行异常:")
    logger.error(f"  任务ID: {job.id}")
    logger.error(f"  任务函数: {job.func_name}")
    logger.error(f"  异常类型: {exc_type.__name__}")
    logger.error(f"  异常信息: {str(exc_value)}")
    return True  # 返回True表示异常已被处理

def signal_handler(signum, frame):
    """信号处理器 - 优雅关闭"""
    logger.info(f"接收到信号 {signum}，准备关闭Worker...")
    sys.exit(0)

def start_worker():
    """启动 RQ Worker - 兼容版本"""
    redis_client = create_redis_connection()
    
    # 注册信号处理器
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    
    worker_name = f'worker-{os.getpid()}'
    logger.info(f"启动Worker: {worker_name}")
    
    try:
        # 创建队列
        queue = Queue('default', connection=redis_client)
        
        # Windows 平台使用 SimpleWorker
        if platform.system() == 'Windows':
            logger.info("检测到 Windows 系统，使用 SimpleWorker")
            from rq import SimpleWorker
            worker = SimpleWorker(
                [queue],
                connection=redis_client,
                name=worker_name,
                exception_handlers=[handle_worker_exception]
            )
        else:
            # Unix/Linux/Mac 使用标准 Worker
            worker = Worker(
                [queue],
                connection=redis_client,
                name=worker_name,
                exception_handlers=[handle_worker_exception]
            )
        
        logger.info(f"Worker {worker_name} 开始监听队列: ['default']")
        logger.info("按 Ctrl+C 停止Worker")
        
        # 开始工作循环
        worker.work()
        
    except KeyboardInterrupt:
        logger.info("接收到中断信号，正在停止Worker...")
    except Exception as e:
        logger.error(f"Worker运行异常: {str(e)}")
        logger.error("可能的解决方案:")
        logger.error("1. 检查RQ版本: pip show rq")
        logger.error("2. 更新RQ: pip install -U rq")
        logger.error("3. 检查Redis服务是否正常运行")
        raise
    finally:
        logger.info(f"Worker {worker_name} 已停止")
def get_worker_info():
    """获取Worker信息"""
    redis_client = create_redis_connection()
    
    try:
        workers = Worker.all(connection=redis_client)
        if workers:
            print(f"发现 {len(workers)} 个活跃Worker:")
            for worker in workers:
                print(f"  - {worker.name}: {worker.state}")
        else:
            print("没有发现活跃的Worker")
        
        # 显示队列信息
        queue = Queue('default', connection=redis_client)
        print(f"\n队列 'default' 状态:")
        print(f"  排队任务: {len(queue)}")
        print(f"  进行中任务: {len(queue.started_job_registry)}")
        print(f"  失败任务: {len(queue.failed_job_registry)}")
        print(f"  完成任务: {len(queue.finished_job_registry)}")
    
    except Exception as e:
        logger.error(f"获取Worker信息失败: {str(e)}")
        print(f"获取Worker信息失败: {str(e)}")

def clear_failed_jobs():
    """清理失败的任务"""
    redis_client = create_redis_connection()
    
    try:
        queue = Queue('default', connection=redis_client)
        failed_count = len(queue.failed_job_registry)
        
        if failed_count > 0:
            queue.failed_job_registry.empty()
            logger.info(f"清理了 {failed_count} 个失败任务")
            print(f"已清理 {failed_count} 个失败任务")
        else:
            print("没有失败任务需要清理")
            
    except Exception as e:
        logger.error(f"清理失败任务时出错: {str(e)}")
        print(f"清理失败任务时出错: {str(e)}")

def check_rq_version():
    """检查RQ版本"""
    try:
        import rq
        print(f"RQ版本: {rq.__version__}")
        
        # 检查关键功能
        from rq import Worker, Queue, Job
        print("✅ RQ核心模块导入成功")
        
        try:
            from rq.connections import push_connection, pop_connection
            print("✅ 新版RQ连接管理可用")
        except ImportError:
            print("⚠️  使用旧版RQ连接管理")
            
        try:
            from rq import Connection
            print("✅ Connection类可用（旧版）")
        except ImportError:
            print("⚠️  Connection类不可用（新版）")
            
    except Exception as e:
        print(f"❌ RQ检查失败: {str(e)}")

def main():
    """主函数 - 支持命令行参数"""
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == 'info':
            get_worker_info()
        elif command == 'clear-failed':
            clear_failed_jobs()
        elif command == 'check':
            check_rq_version()
        elif command == 'help':
            print("Worker管理工具")
            print("用法:")
            print("  python worker.py           # 启动Worker")
            print("  python worker.py info      # 查看Worker信息")
            print("  python worker.py clear-failed  # 清理失败任务")
            print("  python worker.py check     # 检查RQ版本和兼容性")
            print("  python worker.py help      # 显示帮助")
        else:
            print(f"未知命令: {command}")
            print("使用 'python worker.py help' 查看帮助")
    else:
        # 默认启动Worker
        logger.info("=== RQ Worker 启动 ===")
        start_worker()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
        sys.exit(1)
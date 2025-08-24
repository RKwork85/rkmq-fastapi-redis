# run_scripts.py
import subprocess
import time

# 执行 main.py
subprocess.run(["python", "main.py"])

# 执行 work.py
subprocess.run(["python", "work.py"])

# 延迟 3 秒
time.sleep(3)

# 执行 full_task_test.py
subprocess.run(["python", "full_task_test.py"])

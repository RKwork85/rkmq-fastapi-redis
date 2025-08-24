'''
* @Description: 视频切割+混剪一体化处理：先进行场景检测切割，再进行视频混剪
* @Author: rkwork
* @Date: 2025-08-16
* @LastEditTime: 2025-08-16
'''
import os
import random
import math
import concurrent.futures
import logging
import time
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import sys
from collections import defaultdict
from multiprocessing import cpu_count
from scenedetect import VideoManager, SceneManager
from scenedetect.detectors import ContentDetector
from moviepy import VideoFileClip, concatenate_videoclips
from matplotlib.ticker import MaxNLocator
from matplotlib.font_manager import FontProperties
from matplotlib.backends.backend_pdf import PdfPages
from redis import Redis
from rq import get_current_job
from redis.exceptions import AuthenticationError, ConnectionError, TimeoutError
import uuid
from datetime import datetime


def ensure_dir(file_path):
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory, exist_ok=True)


# 设置日志记录器
def setup_logger(output_folder):
    """配置并返回日志记录器"""
    log_file = os.path.join(output_folder, "video_processor.log")
    ensure_dir(log_file)
    
    # 创建日志记录器
    logger = logging.getLogger("VideoProcessor")
    logger.setLevel(logging.DEBUG)
    
    # 创建文件处理器 (使用UTF-8编码)
    file_handler = logging.FileHandler(log_file, mode='w', encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    
    # 创建控制台处理器 (处理编码问题)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # 创建格式化器
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # 添加处理器到记录器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger


class VideoSceneCutter:
    """视频场景检测和切割类"""
    
    def __init__(self, logger=None):
        self.logger = logger or logging.getLogger(__name__)
    
    def detect_scenes(self, video_path, threshold=30.0):
        """检测视频场景切换，返回片段起止时间（秒）"""
        try:
            video_manager = VideoManager([video_path])
            scene_manager = SceneManager()
            scene_manager.add_detector(ContentDetector(threshold=threshold))
            
            video_manager.set_downscale_factor()
            video_manager.start()
            scene_manager.detect_scenes(frame_source=video_manager)
            
            scene_list = scene_manager.get_scene_list()
            scene_times = [(start.get_seconds(), end.get_seconds()) 
                          for start, end in scene_list]
            
            self.logger.info(f"检测到 {len(scene_times)} 个场景片段: {os.path.basename(video_path)}")
            return scene_times
        
        except Exception as e:
            self.logger.error(f"场景检测失败 {video_path}: {str(e)}")
            return []
        finally:
            if 'video_manager' in locals():
                video_manager.release()

    def process_single_clip(self, video_path, start, end, output_path):
        """处理单个视频片段（优化内存使用）"""
        try:
            with VideoFileClip(video_path) as video:
                clip = video.subclipped(start, end)
                clip.write_videofile(
                    output_path,
                    codec='libx264',
                    audio_codec='aac',
                    preset='fast',
                    ffmpeg_params=['-threads', '2'],
                    logger=None
                )
            self.logger.info(f"导出完成: {output_path} 时长: {end-start:.2f}s")
            return True
        except Exception as e:
            self.logger.error(f"片段导出失败 {output_path}: {str(e)}")
            return False

    def process_video(self, video_path, output_root, threshold=30.0):
        """处理单个视频的全流程"""
        start_time = time.time()
        base_name = os.path.splitext(os.path.basename(video_path))[0]
        
        # 场景检测
        scene_times = self.detect_scenes(video_path, threshold)
        if not scene_times:
            return False
        
        # 过滤短片段
        valid_scenes = [(s, e) for s, e in scene_times if e - s > 1.5]
        
        # 并行处理片段
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = []
            for i, (start, end) in enumerate(valid_scenes):
                # 直接使用输出根目录，文件名包含原视频名和片段序号
                output_path = os.path.join(output_root, f"{base_name}_clip_{i+1:03d}.mp4")
                futures.append(
                    executor.submit(
                        self.process_single_clip,
                        video_path, start, end, output_path
                    )
                )
            
            # 等待所有任务完成
            results = [f.result() for f in futures]
        
        success = all(results)
        elapsed = time.time() - start_time
        self.logger.info(f"视频处理完成: {video_path} | 耗时: {elapsed:.1f}s | 成功片段: {sum(results)}/{len(results)}")
        return success

    def cut_videos_to_clips(self, video_dir, output_root, threshold=30.0, max_concurrent_videos=None):
        """将文件夹下的视频切割成片段"""
        if max_concurrent_videos is None:
            max_concurrent_videos = max(1, cpu_count() // 2)
            
        # 创建输出目录
        os.makedirs(output_root, exist_ok=True)
        
        self.logger.info(f"开始视频切割任务")
        self.logger.info(f"所有片段将输出到目录: {output_root}")
        
        # 获取视频文件列表
        video_files = []
        for ext in ['.mp4', '.avi', '.mov', '.mkv']:
            video_files.extend([
                os.path.join(video_dir, f) 
                for f in os.listdir(video_dir) 
                if f.lower().endswith(ext)
            ])
        
        if not video_files:
            self.logger.warning(f"未发现视频文件: {video_dir}")
            return False
        
        self.logger.info(f"开始处理 {len(video_files)} 个视频 | 并发数: {max_concurrent_videos}")
        
        # 使用进程池处理多个视频
        with concurrent.futures.ProcessPoolExecutor(
            max_workers=max_concurrent_videos
        ) as executor:
            # 准备任务参数
            tasks = {executor.submit(self.process_video, path, output_root, threshold): path 
                    for path in video_files}
            
            # 处理任务结果
            success_count = 0
            for future in concurrent.futures.as_completed(tasks):
                video_path = tasks[future]
                try:
                    if future.result():
                        success_count += 1
                except Exception as e:
                    self.logger.error(f"视频处理异常 {video_path}: {str(e)}")
        
        self.logger.info(f"切割完成 | 成功: {success_count}/{len(video_files)} | 输出目录: {output_root}")
        return success_count > 0


class VideoCombinator:
    def __init__(self, input_folder, output_folder, min_duration=60, max_duration=300, 
                 max_clips_per_video=5, max_videos=50, max_usage=3,ttf_path="fonts/汉仪晓波花月圆W.ttf", logger=None):
        """
        初始化视频组合器
        
        参数:
        input_folder: 输入素材文件夹
        output_folder: 输出视频文件夹
        min_duration: 合成视频最小时长(秒)
        max_duration: 合成视频最大时长(秒)
        max_clips_per_video: 每个合成视频最多包含的素材数量
        max_videos: 最大生成视频数量
        max_usage: 每个素材最多被使用次数
        logger: 日志记录器实例
        """
        self.input_folder = input_folder
        self.output_folder = output_folder
        self.min_duration = min_duration
        self.max_duration = max_duration
        self.max_clips_per_video = max_clips_per_video
        self.max_videos = max_videos
        self.max_usage = max_usage
        self.ttf_path  = ttf_path 
        
        # 设置日志记录器
        self.logger = logger if logger else setup_logger(output_folder)
        self.logger.info(f"初始化视频组合器: 输入目录={input_folder}, 输出目录={output_folder}")
        self.logger.info(f"参数: 最小时长={min_duration}s, 最大时长={max_duration}s, "
                         f"最大片段数={max_clips_per_video}, 最大视频数={max_videos}, 最大使用次数={max_usage}")
        
        # 初始化数据结构
        self.usage_counter = defaultdict(int)
        self.generated_count = 0
        self.combination_history = set()
        self.video_records = []  # 视频记录
        self.clip_records = []    # 素材记录
        
        # 加载视频素材
        self.video_clips = self._load_video_clips()
        self.logger.info(f"成功加载 {len(self.video_clips)} 个视频素材")
        
    def _load_video_clips(self):
        """加载文件夹中的所有视频素材并获取真实时长"""
        clips = []
        self.logger.info(f"开始扫描输入目录: {self.input_folder}")
        
        for filename in os.listdir(self.input_folder):
            if filename.lower().endswith(('.mp4', '.mov', '.avi', '.mkv', '.flv', '.webm')):
                path = os.path.join(self.input_folder, filename)
                try:
                    # 使用MoviePy获取真实视频时长
                    with VideoFileClip(path) as clip:
                        duration = clip.duration
                    
                    # 创建素材记录
                    clip_id = os.path.splitext(filename)[0]
                    clip_record = {
                        '素材ID': clip_id,
                        '素材名称': filename,
                        '素材路径': path,
                        '时长(秒)': duration,
                        '使用次数': 0
                    }
                    self.clip_records.append(clip_record)
                    
                    # 添加到可用素材列表
                    clips.append({
                        'path': path,
                        'duration': duration,
                        'name': filename,
                        'id': clip_id
                    })
                    
                    self.logger.debug(f"加载视频: {filename} (时长: {duration:.2f}s)")
                    
                except Exception as e:
                    self.logger.error(f"无法加载视频 {filename}: {str(e)}", exc_info=True)
        
        if not clips:
            self.logger.warning("输入目录中没有找到任何视频文件!")
        return clips
    
    def _calculate_max_combinations(self):
        """计算理论上可能的组合数量(仅用于信息展示)"""
        n = len(self.video_clips)
        k = self.max_clips_per_video
        
        if n == 0 or k == 0:
            return 0
        
        # 排列组合公式: P(n, k) = n! / (n-k)!
        # permutations = math.factorial(n) // math.factorial(n - k)
        
        # 考虑不同长度组合
        total_combinations = 0
        for i in range(1, k + 1):
            try:
                # 组合数 * 排列数
                total_combinations += math.comb(n, i) * math.factorial(i)
            except ValueError:
                # 处理n < i的情况
                pass
        
        return total_combinations
    
    def _can_add_clip(self, selected_clips, candidate):
        """检查是否可以添加候选片段"""
        # 检查是否已选择
        if candidate in selected_clips:
            return False
            
        # 检查使用次数限制
        if self.usage_counter[candidate['path']] >= self.max_usage:
            self.logger.debug(f"素材已达到最大使用次数: {candidate['name']}")
            return False
            
        # 计算当前总时长
        current_duration = sum(c['duration'] for c in selected_clips)
        new_duration = current_duration + candidate['duration']
        
        # 检查时长限制
        if new_duration > self.max_duration:
            self.logger.debug(f"添加 {candidate['name']} 将超出最大时长限制 "
                             f"({new_duration:.1f} > {self.max_duration})")
            return False
            
        # 视觉节奏控制: 避免连续相似时长素材
        if selected_clips:
            last_duration = selected_clips[-1]['duration']
            # 如果连续两个素材时长非常接近（小于1秒差），则跳过
            if abs(last_duration - candidate['duration']) < 1:
                self.logger.debug(f"跳过 {candidate['name']}: 与前一片段时长相似 "
                                 f"({last_duration:.1f}s vs {candidate['duration']:.1f}s)")
                return False
                
        return True
    
    def _generate_video_combination(self):
        """
        生成一组视频组合方案
        算法特点:
        1. 多样性优先: 随机选择素材数量(1到max_clips_per_video)
        2. 时长控制: 确保总时长在[min_duration, max_duration]范围内
        3. 使用均衡: 优先选择使用次数少的素材
        4. 顺序随机化: 随机排列组合顺序
        5. 视觉差异: 避免连续相似时长素材(创建节奏变化)
        6. 历史记录: 避免生成完全相同的组合
        """
        # 如果已达到最大视频数
        if self.generated_count >= self.max_videos:
            self.logger.debug("已达到最大视频数量限制")
            return None
            
        # 按使用次数排序(使用次数少的优先)
        available_clips = sorted(
            [clip for clip in self.video_clips if self.usage_counter[clip['path']] < self.max_usage],
            key=lambda x: self.usage_counter[x['path']]
        )
        
        if not available_clips:
            self.logger.warning("没有可用素材: 所有素材已达到最大使用次数")
            return None
        
        self.logger.debug(f"生成新组合: 可用素材={len(available_clips)}, 已生成={self.generated_count}/{self.max_videos}")
        
        # 确定本次组合包含的素材数量(1到max_clips_per_video)
        num_clips = random.randint(1, self.max_clips_per_video)
        self.logger.debug(f"目标片段数量: {num_clips}")
        
        # 尝试生成组合
        selected_clips = []
        total_duration = 0
        attempt_count = 0
        max_attempts = 200  # 防止无限循环
        
        while attempt_count < max_attempts:
            # 如果已有素材但还没达到最小长度，优先选择较长的素材
            if selected_clips and total_duration < self.min_duration:
                # 选择时长最长的前10个素材
                candidates = sorted(
                    [c for c in available_clips if c not in selected_clips],
                    key=lambda x: x['duration'], 
                    reverse=True
                )[:10]
                
                if candidates:
                    candidate = random.choice(candidates)
                    self.logger.debug(f"尝试添加长片段以满足时长: {candidate['name']} ({candidate['duration']:.1f}s)")
                else:
                    candidate = None
            else:
                # 优先选择使用次数少的素材
                candidate_pool = available_clips[:max(10, len(available_clips)//2)]
                if candidate_pool:
                    candidate = random.choice(candidate_pool)
                    self.logger.debug(f"随机选择片段: {candidate['name']} (使用次数: {self.usage_counter[candidate['path']]}/{self.max_usage})")
                else:
                    candidate = None
            
            if not candidate:
                attempt_count += 1
                continue
            
            # 检查是否可以添加
            if self._can_add_clip(selected_clips, candidate):
                selected_clips.append(candidate)
                total_duration += candidate['duration']
                self.logger.debug(f"添加片段: {candidate['name']} (当前时长: {total_duration:.1f}s, 片段数: {len(selected_clips)})")
                
                # 如果达到最小时长要求，可以提前结束
                if total_duration >= self.min_duration and len(selected_clips) >= 2:
                    self.logger.debug(f"达到最小时长要求 ({total_duration:.1f}s >= {self.min_duration}s)")
                    break
            else:
                attempt_count += 1
        
        # 检查是否满足时长要求
        if total_duration < self.min_duration or not selected_clips:
            self.logger.warning(f"无法生成有效组合: 总时长不足 ({total_duration:.1f}s < {self.min_duration}s) 或没有选择片段")
            return None
            
        # 创建组合指纹（避免重复组合）
        clip_ids = tuple(sorted(c['path'] for c in selected_clips))
        if clip_ids in self.combination_history:
            self.logger.warning(f"跳过重复组合: {clip_ids}")
            return None  # 跳过重复组合
            
        self.combination_history.add(clip_ids)
        
        # 随机排列顺序以增加差异性
        random.shuffle(selected_clips)
        
        # 更新使用计数器
        for clip in selected_clips:
            self.usage_counter[clip['path']] += 1
            
        self.generated_count += 1
        
        clip_names = [c['name'] for c in selected_clips]
        clip_ids = [c['id'] for c in selected_clips]
        self.logger.info(f"生成新组合 #{self.generated_count}: {clip_names} (总时长: {total_duration:.1f}s)")
            
        return {
            'clips': selected_clips,
            'total_duration': total_duration,
            'clip_names': clip_names,
            'clip_ids': clip_ids
        }
    
    def _render_video(self, combination, index, start_time):
        """使用MoviePy渲染单个视频"""
        video_info = {
            '视频ID': f"combo_{index:03d}",
            '视频名称': f"combo_{index:03d}.mp4",
            '视频路径': '',
            '视频时长(秒)': combination['total_duration'],
            '渲染状态': '失败',
            '渲染耗时(秒)': 0,
            '使用素材列表': ', '.join(combination['clip_names']),
            '素材ID列表': ', '.join(combination['clip_ids']),
            '素材数量': len(combination['clips']),
            '单个处理时长(秒)': 0,
            '总处理时长(秒)': 0,
            '开始时间': time.time(),
            '结束时间': 0
        }
        
        clips = []
        final_clip = None
        try:
            clip_names = [c['name'] for c in combination['clips']]
            self.logger.info(f"开始渲染视频 #{index}: 包含片段 {clip_names}")
            
            # 记录开始时间
            render_start = time.time()
            
            # 加载所有视频片段
            for clip_info in combination['clips']:
                self.logger.debug(f"加载片段: {clip_info['name']}")
                clip = VideoFileClip(clip_info['path'])
                clips.append(clip)
            
            # 合成视频
            self.logger.debug(f"拼接 {len(clips)} 个片段")
            final_clip = concatenate_videoclips(clips, method="compose")
            
            # 输出路径
            output_path = os.path.join(self.output_folder, video_info['视频名称'])
            video_info['视频路径'] = output_path
            self.logger.info(f"开始写入视频文件: {output_path}")
            
            # 写入视频文件（使用多线程加速）
            final_clip.write_videofile(
                output_path, 
                threads=4, 
                logger=None,
                preset='fast',
                ffmpeg_params=['-movflags', '+faststart']
            )
            
            # 记录结束时间
            render_end = time.time()
            
            # 计算时间指标
            render_time = render_end - render_start
            total_process_time = render_end - start_time
            
            # 更新视频信息
            video_info['渲染状态'] = '成功'
            video_info['渲染耗时(秒)'] = render_time
            video_info['单个处理时长(秒)'] = render_time
            video_info['总处理时长(秒)'] = total_process_time
            video_info['结束时间'] = render_end
            
            self.logger.info(f"成功生成视频 #{index}: {output_path} (时长: {combination['total_duration']:.1f}秒, "
                            f"包含 {len(combination['clips'])} 个素材, 渲染耗时: {render_time:.2f}秒)")
            return video_info
        except Exception as e:
            # 记录错误信息
            render_end = time.time()
            video_info['结束时间'] = render_end
            video_info['渲染耗时(秒)'] = render_end - video_info['开始时间']
            video_info['总处理时长(秒)'] = render_end - start_time
            video_info['错误信息'] = str(e)
            
            self.logger.error(f"渲染视频失败: {str(e)}", exc_info=True)
            return video_info
        finally:
            # 确保关闭所有资源
            for clip in clips:
                try:
                    clip.close()
                except:
                    pass
            if final_clip:
                try:
                    final_clip.close()
                except:
                    pass
    def _create_excel_report(self, video_data):
        """创建Excel报告"""
        if not video_data:
            self.logger.warning("没有视频数据可生成报告")
            return None
            
        # 创建输出目录
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Excel文件路径
        excel_path = os.path.join(self.output_folder, "视频生产报告.xlsx")
        
        try:
            # 创建DataFrame
            df = pd.DataFrame(video_data)
            
            # 添加序号列
            df.insert(0, '序号', range(1, len(df) + 1))
            
            # 计算平均值
            avg_values = {
                '视频时长(秒)': df['视频时长(秒)'].mean(),
                '渲染耗时(秒)': df['渲染耗时(秒)'].mean(),
                '单个处理时长(秒)': df['单个处理时长(秒)'].mean(),
                '总处理时长(秒)': df['总处理时长(秒)'].mean(),
                '素材数量': df['素材数量'].mean()
            }
            
            # 设置Excel写入器
            writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
            df.to_excel(writer, sheet_name='视频生产报告', index=False)
            
            # 获取工作簿和工作表对象
            workbook = writer.book
            worksheet = writer.sheets['视频生产报告']
            
            # 设置列宽
            column_widths = {
                '序号': 8,
                '视频ID': 15,
                '视频名称': 20,
                '视频路径': 50,
                '视频时长(秒)': 15,
                '渲染状态': 12,
                '渲染耗时(秒)': 15,
                '使用素材列表': 40,
                '素材ID列表': 40,
                '素材数量': 12,
                '单个处理时长(秒)': 18,
                '总处理时长(秒)': 18
            }
            
            # 应用列宽
            for col_name, width in column_widths.items():
                if col_name in df.columns:
                    col_idx = df.columns.get_loc(col_name)
                    worksheet.set_column(col_idx, col_idx, width)
            
            # 添加条件格式 - 根据渲染状态着色
            format_success = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
            format_failure = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            
            if '渲染状态' in df.columns:
                status_col_idx = df.columns.get_loc('渲染状态')
                for row_idx in range(1, len(df) + 1):
                    cell_value = df.iloc[row_idx - 1]['渲染状态']
                    if cell_value == '成功':
                        worksheet.conditional_format(row_idx, status_col_idx, row_idx, status_col_idx, {
                            'type': 'cell',
                            'criteria': 'equal to',
                            'value': '"成功"',
                            'format': format_success
                        })
                    else:
                        worksheet.conditional_format(row_idx, status_col_idx, row_idx, status_col_idx, {
                            'type': 'cell',
                            'criteria': 'equal to',
                            'value': '"失败"',
                            'format': format_failure
                        })
            
            # 添加数据条格式 - 渲染时长
            if '渲染耗时(秒)' in df.columns:
                time_col_idx = df.columns.get_loc('渲染耗时(秒)')
                worksheet.conditional_format(1, time_col_idx, len(df), time_col_idx, {
                    'type': 'data_bar',
                    'bar_color': '#63C384'
                })
            
            # 添加汇总统计
            stats_data = {
                '指标': ['总视频数', '成功数', '失败数', '平均时长(秒)', '平均渲染时间(秒)', '平均素材数'],
                '值': [
                    len(df),
                    len(df[df['渲染状态'] == '成功']),
                    len(df[df['渲染状态'] == '失败']),
                    avg_values['视频时长(秒)'],
                    avg_values['渲染耗时(秒)'],
                    avg_values['素材数量']
                ]
            }
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='汇总统计', index=False)
            
            # 获取汇总统计工作表
            stats_worksheet = writer.sheets['汇总统计']
            
            # 设置汇总统计列宽
            stats_worksheet.set_column(0, 0, 20)
            stats_worksheet.set_column(1, 1, 15)
            
            # 添加饼图 - 渲染状态分布
            if len(df) > 0 and '渲染状态' in df.columns:
                success_count = len(df[df['渲染状态'] == '成功'])
                failure_count = len(df[df['渲染状态'] == '失败'])
                
                if success_count + failure_count > 0:
                    chart = workbook.add_chart({'type': 'pie'})
                    chart.add_series({
                        'name': '渲染状态分布',
                        'categories': ['汇总统计', 1, 0, 2, 0],
                        'values': ['汇总统计', 1, 1, 2, 1],
                    })
                    chart.set_title({'name': '渲染状态分布'})
                    stats_worksheet.insert_chart('D2', chart)
            
            # 添加柱状图 - 渲染时间分布
            if '渲染耗时(秒)' in df.columns and len(df) > 0:
                chart = workbook.add_chart({'type': 'column'})
                chart.add_series({
                    'name': '渲染时间(秒)',
                    'categories': ['视频生产报告', 1, 0, len(df), 0],
                    'values': ['视频生产报告', 1, time_col_idx, len(df), time_col_idx],
                })
                chart.set_title({'name': '渲染时间分布'})
                chart.set_x_axis({'name': '视频ID'})
                chart.set_y_axis({'name': '时间(秒)'})
                worksheet.insert_chart('M2', chart)
            
            # 保存Excel文件
            writer.close()
            
            self.logger.info(f"成功生成Excel报告: {excel_path}")
            return excel_path
        except Exception as e:
            self.logger.error(f"生成Excel报告失败: {str(e)}", exc_info=True)
            return None

        """创建Excel报告"""
        if not video_data:
            self.logger.warning("没有视频数据可生成报告")
            return None
            
        # 创建输出目录
        os.makedirs(self.output_folder, exist_ok=True)
        
        # Excel文件路径
        excel_path = os.path.join(self.output_folder, "视频生产报告.xlsx")
        
        try:
            # 创建DataFrame
            df = pd.DataFrame(video_data)
            
            # 添加序号列
            df.insert(0, '序号', range(1, len(df) + 1))
            
            # 计算平均值
            avg_values = {
                '视频时长(秒)': df['视频时长(秒)'].mean(),
                '渲染耗时(秒)': df['渲染耗时(秒)'].mean(),
                '单个处理时长(秒)': df['单个处理时长(秒)'].mean(),
                '总处理时长(秒)': df['总处理时长(秒)'].mean(),
                '素材数量': df['素材数量'].mean()
            }
            
            # 设置Excel写入器
            writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
            df.to_excel(writer, sheet_name='视频生产报告', index=False)
            
            # 获取工作簿和工作表对象
            workbook = writer.book
            worksheet = writer.sheets['视频生产报告']
            
            # 设置列宽
            column_widths = {
                '序号': 8,
                '视频ID': 15,
                '视频名称': 20,
                '视频路径': 50,
                '视频时长(秒)': 15,
                '渲染状态': 12,
                '渲染耗时(秒)': 15,
                '使用素材列表': 40,
                '素材ID列表': 40,
                '素材数量': 12,
                '单个处理时长(秒)': 18,
                '总处理时长(秒)': 18
            }
            
            # 应用列宽
            for col_name, width in column_widths.items():
                if col_name in df.columns:
                    col_idx = df.columns.get_loc(col_name)
                    worksheet.set_column(col_idx, col_idx, width)
            
            # 添加条件格式 - 根据渲染状态着色
            format_success = workbook.add_format({'bg_color': '#C6EFCE', 'font_color': '#006100'})
            format_failure = workbook.add_format({'bg_color': '#FFC7CE', 'font_color': '#9C0006'})
            
            if '渲染状态' in df.columns:
                status_col_idx = df.columns.get_loc('渲染状态')
                for row_idx in range(1, len(df) + 1):
                    cell_value = df.iloc[row_idx - 1]['渲染状态']
                    if cell_value == '成功':
                        worksheet.conditional_format(row_idx, status_col_idx, row_idx, status_col_idx, {
                            'type': 'cell',
                            'criteria': 'equal to',
                            'value': '"成功"',
                            'format': format_success
                        })
                    else:
                        worksheet.conditional_format(row_idx, status_col_idx, row_idx, status_col_idx, {
                            'type': 'cell',
                            'criteria': 'equal to',
                            'value': '"失败"',
                            'format': format_failure
                        })
            
            # 添加数据条格式 - 渲染时长
            if '渲染耗时(秒)' in df.columns:
                time_col_idx = df.columns.get_loc('渲染耗时(秒)')
                worksheet.conditional_format(1, time_col_idx, len(df), time_col_idx, {
                    'type': 'data_bar',
                    'bar_color': '#63C384'
                })
            
            # 添加汇总统计
            stats_data = {
                '指标': ['总视频数', '成功数', '失败数', '平均时长(秒)', '平均渲染时间(秒)', '平均素材数'],
                '值': [
                    len(df),
                    len(df[df['渲染状态'] == '成功']),
                    len(df[df['渲染状态'] == '失败']),
                    avg_values['视频时长(秒)'],
                    avg_values['渲染耗时(秒)'],
                    avg_values['素材数量']
                ]
            }
            
            stats_df = pd.DataFrame(stats_data)
            stats_df.to_excel(writer, sheet_name='汇总统计', index=False)
            
            # 获取汇总统计工作表
            stats_worksheet = writer.sheets['汇总统计']
            
            # 设置汇总统计列宽
            stats_worksheet.set_column(0, 0, 20)
            stats_worksheet.set_column(1, 1, 15)
            
            # 保存Excel文件
            writer.close()
            
            self.logger.info(f"成功生成Excel报告: {excel_path}")
            return excel_path
        except Exception as e:
            self.logger.error(f"生成Excel报告失败: {str(e)}", exc_info=True)
            return None
    

    def _create_clip_report(self):
        """创建素材使用报告"""
        if not self.clip_records:
            self.logger.warning("没有素材数据可生成报告")
            return None
            
        try:
            # 创建输出目录
            os.makedirs(self.output_folder, exist_ok=True)
            
            # Excel文件路径
            excel_path = os.path.join(self.output_folder, "素材使用报告.xlsx")
            
            # 创建DataFrame
            df = pd.DataFrame(self.clip_records)
            
            # 添加序号列
            df.insert(0, '序号', range(1, len(df) + 1))
            
            # 设置Excel写入器
            writer = pd.ExcelWriter(excel_path, engine='xlsxwriter')
            df.to_excel(writer, sheet_name='素材使用报告', index=False)
            
            # 获取工作簿和工作表对象
            workbook = writer.book
            worksheet = writer.sheets['素材使用报告']
            
            # 设置列宽
            column_widths = {
                '序号': 8,
                '素材ID': 20,
                '素材名称': 30,
                '素材路径': 50,
                '时长(秒)': 15,
                '使用次数': 15
            }
            
            # 应用列宽
            for col_name, width in column_widths.items():
                if col_name in df.columns:
                    col_idx = df.columns.get_loc(col_name)
                    worksheet.set_column(col_idx, col_idx, width)
            
            # 添加数据条格式 - 使用次数
            if '使用次数' in df.columns:
                usage_col_idx = df.columns.get_loc('使用次数')
                worksheet.conditional_format(1, usage_col_idx, len(df), usage_col_idx, {
                    'type': 'data_bar',
                    'bar_color': '#FFA500'
                })
            
            # 保存Excel文件
            writer.close()
            
            self.logger.info(f"成功生成素材使用报告: {excel_path}")
            return excel_path
        except Exception as e:
            self.logger.error(f"生成素材使用报告失败: {str(e)}", exc_info=True)
            return None    

    def analyze_and_visualize_clips(self):
        """分析并可视化素材使用情况和时长分布"""
        if not self.clip_records:
            self.logger.warning("没有素材数据可进行分析")
            return None
            
        try:
            # 创建输出目录
            os.makedirs(self.output_folder, exist_ok=True)
            
            # 创建图表输出目录
            charts_dir = os.path.join(self.output_folder, "图表")
            os.makedirs(charts_dir, exist_ok=True)
            
            # 创建DataFrame
            df = pd.DataFrame(self.clip_records)
            
            # 1. 素材使用情况分析
            usage_counts = df['使用次数'].value_counts().sort_index()
            unused_clips = len(df[df['使用次数'] == 0])
            fully_used_clips = len(df[df['使用次数'] == self.max_usage])
            
            # 2. 时长分布分析
            durations = df['时长(秒)']
            min_duration = durations.min()
            max_duration = durations.max()
            avg_duration = durations.mean()
            median_duration = durations.median()
            
            # 3. 使用次数与时长相关性分析
            correlation = df[['使用次数', '时长(秒)']].corr().iloc[0, 1]
            
            # 打印分析结果
            self.logger.info("\n" + "=" * 80)
            self.logger.info("素材使用情况分析:")
            self.logger.info(f"  总素材数量: {len(df)}")
            self.logger.info(f"  未使用的素材: {unused_clips} ({unused_clips/len(df)*100:.1f}%)")
            self.logger.info(f"  达到最大使用次数的素材: {fully_used_clips} ({fully_used_clips/len(df)*100:.1f}%)")
            self.logger.info(f"  平均使用次数: {df['使用次数'].mean():.2f}")
            self.logger.info("\n素材时长分布分析:")
            self.logger.info(f"  最短时长: {min_duration:.2f}秒")
            self.logger.info(f"  最长时长: {max_duration:.2f}秒")
            self.logger.info(f"  平均时长: {avg_duration:.2f}秒")
            self.logger.info(f"  中位时长: {median_duration:.2f}秒")
            self.logger.info("\n相关性分析:")
            self.logger.info(f"  使用次数与时长相关性: {correlation:.2f}")
            self.logger.info("=" * 80)
            
            chinese_font_path = self.ttf_path if self.ttf_path else None
            # 创建中文字体对象
            from matplotlib.font_manager import FontProperties
            if chinese_font_path and os.path.exists(chinese_font_path):
                chinese_font = FontProperties(fname=chinese_font_path)
                self.logger.info(f"使用指定中文字体: {chinese_font_path}")
            else:
                # 回退到系统默认中文字体
                chinese_font = FontProperties(family=self.ttf_path)
                self.logger.warning("未指定中文字体路径，使用系统默认字体")
            
            # 设置PDF嵌入参数
            plt.rcParams['pdf.fonttype'] = 42  # 嵌入TrueType字体
            plt.rcParams['ps.fonttype'] = 42   # 同时设置PostScript选项
            
            # 创建图表
            plt.style.use('seaborn-v0_8-whitegrid')
            
            # 图表1: 素材使用次数分布
            plt.figure(figsize=(10, 6))
            usage_counts.plot(kind='bar', color='skyblue')
            plt.title('素材使用次数分布', fontsize=14, fontproperties=chinese_font)
            plt.ylabel('素材数量', fontsize=12, fontproperties=chinese_font)
            plt.xlabel('使用次数', fontsize=12, fontproperties=chinese_font)
            plt.xticks(rotation=0)
            plt.gca().yaxis.set_major_locator(MaxNLocator(integer=True))
            plt.tight_layout()
            usage_dist_path = os.path.join(charts_dir, "使用次数分布.png")
            plt.savefig(usage_dist_path, dpi=150)
            plt.close()
            self.logger.info(f"生成使用次数分布图: {usage_dist_path}")
            
            # 图表2: 素材时长分布直方图
            plt.figure(figsize=(10, 6))
            n_bins = min(20, len(durations.unique()))
            plt.hist(durations, bins=n_bins, color='lightgreen', edgecolor='black', alpha=0.7)
            plt.title('素材时长分布', fontsize=14, fontproperties=chinese_font)
            plt.xlabel('时长 (秒)', fontsize=12, fontproperties=chinese_font)
            plt.ylabel('素材数量', fontsize=12, fontproperties=chinese_font)
            plt.axvline(avg_duration, color='r', linestyle='dashed', linewidth=1, label=f'平均时长: {avg_duration:.2f}秒')
            plt.axvline(median_duration, color='b', linestyle='dashed', linewidth=1, label=f'中位时长: {median_duration:.2f}秒')
            plt.legend(prop=chinese_font)
            plt.tight_layout()
            duration_dist_path = os.path.join(charts_dir, "时长分布.png")
            plt.savefig(duration_dist_path, dpi=150)
            plt.close()
            self.logger.info(f"生成长时分布图: {duration_dist_path}")
            
            # 图表3: 使用次数与时长关系
            plt.figure(figsize=(10, 6))
            plt.scatter(df['时长(秒)'], df['使用次数'], alpha=0.6, color='purple', edgecolors='w')
            plt.title('素材时长与使用次数关系', fontsize=14, fontproperties=chinese_font)
            plt.xlabel('时长 (秒)', fontsize=12, fontproperties=chinese_font)
            plt.ylabel('使用次数', fontsize=12, fontproperties=chinese_font)
            plt.grid(True, linestyle='--', alpha=0.7)
            
            # 添加趋势线
            if len(df) > 1:
                z = np.polyfit(df['时长(秒)'], df['使用次数'], 1)
                p = np.poly1d(z)
                plt.plot(df['时长(秒)'], p(df['时长(秒)']), "r--", label=f'趋势线 (r={correlation:.2f})')
                plt.legend(prop=chinese_font)
            
            plt.tight_layout()
            correlation_path = os.path.join(charts_dir, "时长-使用次数关系.png")
            plt.savefig(correlation_path, dpi=150)
            plt.close()
            self.logger.info(f"生成时长-使用次数关系图: {correlation_path}")
            
            # === 新增: 时长区间分布分析 ===
            # 定义时长区间
            bins = [0, 2, 6, 15, 25, float('inf')]
            labels = ['<2秒', '2-6秒', '6-15秒', '15-25秒', '>25秒']
            
            # 添加时长区间列
            df['时长区间'] = pd.cut(df['时长(秒)'], bins=bins, labels=labels, right=False)
            
            # 统计各区间素材数量
            interval_counts = df['时长区间'].value_counts().reindex(labels, fill_value=0)
            
            # 为每个区间收集素材名称
            interval_clips = {}
            for interval in labels:
                clips_in_interval = df[df['时长区间'] == interval]
                clip_names = clips_in_interval['素材名称'].tolist()
                # 如果素材太多，只取前5个
                if len(clip_names) > 5:
                    clip_names = clip_names[:5] + ['等...']
                interval_clips[interval] = clip_names
            
            # 绘制时长区间分布图
            plt.figure(figsize=(12, 8))
            ax = interval_counts.plot(kind='bar', color='teal')
            plt.title('素材时长区间分布', fontsize=16, fontproperties=chinese_font)
            plt.xlabel('时长区间', fontsize=12, fontproperties=chinese_font)
            plt.ylabel('素材数量', fontsize=12, fontproperties=chinese_font)
            plt.xticks(rotation=0)
            
            # 在柱子上方显示数量
            for p in ax.patches:
                ax.annotate(str(p.get_height()), 
                        (p.get_x() + p.get_width() / 2., p.get_height()),
                        ha='center', va='center', 
                        xytext=(0, 5), 
                        textcoords='offset points')
            
            # 在图表右侧添加素材列表
            clip_text = ""
            for interval in labels:
                clip_text += f"{interval}:\n"
                for clip in interval_clips[interval]:
                    clip_text += f"  • {clip}\n"
                clip_text += "\n"
            
            plt.figtext(0.75, 0.5, clip_text, 
                    fontproperties=chinese_font,
                    fontsize=10,
                    bbox=dict(facecolor='white', alpha=0.5))
            
            plt.tight_layout()
            plt.subplots_adjust(right=0.7)  # 为素材列表留出空间
            duration_interval_path = os.path.join(charts_dir, "时长区间分布.png")
            plt.savefig(duration_interval_path, dpi=150)
            plt.close()
            self.logger.info(f"生成长时区间分布图: {duration_interval_path}")
            # === 结束新增部分 ===
            
            # 图表4: 最常用素材 - 修改：添加时长信息
            top_clips = df.sort_values('使用次数', ascending=False).head(10)
            # 创建带时长的标签
            clip_labels = [f"{name} ({duration:.1f}s)" 
                        for name, duration in zip(top_clips['素材名称'], top_clips['时长(秒)'])]
            
            plt.figure(figsize=(12, 7))
            plt.barh(clip_labels, top_clips['使用次数'], color='orange')
            plt.title('最常用素材 (Top 10)', fontsize=14, fontproperties=chinese_font)
            plt.xlabel('使用次数', fontsize=12, fontproperties=chinese_font)
            # 不需要再设置ylabel，因为标签已经在纵坐标上了
            plt.gca().invert_yaxis()
            plt.tight_layout()
            top_clips_path = os.path.join(charts_dir, "最常用素材.png")
            plt.savefig(top_clips_path, dpi=150)
            plt.close()
            self.logger.info(f"生成最常用素材图: {top_clips_path}")
            
            # 图表5: 时长箱线图
            plt.figure(figsize=(10, 6))
            plt.boxplot(durations, vert=False, patch_artist=True, 
                        boxprops=dict(facecolor='lightblue'))
            plt.title('素材时长箱线图', fontsize=14, fontproperties=chinese_font)
            plt.xlabel('时长 (秒)', fontsize=12, fontproperties=chinese_font)
            plt.tight_layout()
            boxplot_path = os.path.join(charts_dir, "时长箱线图.png")
            plt.savefig(boxplot_path, dpi=150)
            plt.close()
            self.logger.info(f"生成长时箱线图: {boxplot_path}")
            
            # 创建分析报告
            report_path = os.path.join(self.output_folder, "素材分析报告.pdf")
            
            with PdfPages(report_path) as pdf:
                # 封面 - 显式使用中文字体
                plt.figure(figsize=(11, 8.5))
                plt.text(0.5, 0.7, '素材使用情况分析报告', 
                        fontsize=24, ha='center', va='center', fontproperties=chinese_font)
                plt.text(0.5, 0.5, f'生成时间: {time.strftime("%Y-%m-%d %H:%M:%S")}', 
                        fontsize=16, ha='center', va='center', fontproperties=chinese_font)
                plt.text(0.5, 0.4, f'素材总数: {len(self.clip_records)}', 
                        fontsize=16, ha='center', va='center', fontproperties=chinese_font)
                plt.axis('off')
                pdf.savefig(bbox_inches='tight', pad_inches=0.1)
                plt.close()
                
                # 新增：时长区间分布作为第一页内容页
                try:
                    img = plt.imread(duration_interval_path)
                    plt.figure(figsize=(11, 8.5))
                    plt.imshow(img)
                    plt.axis('off')
                    plt.title('素材时长区间分布', fontsize=18, fontproperties=chinese_font, pad=20)
                    pdf.savefig(bbox_inches='tight', pad_inches=0.1)
                    plt.close()
                except Exception as e:
                    self.logger.error(f"无法加载时长区间分布图: {str(e)}")
                
                # 添加其他图表
                for fig_path in [usage_dist_path, duration_dist_path, correlation_path, 
                                top_clips_path, boxplot_path]:
                    try:
                        img = plt.imread(fig_path)
                        plt.figure(figsize=(11, 8.5))
                        plt.imshow(img)
                        plt.axis('off')
                        pdf.savefig(bbox_inches='tight', pad_inches=0.1)
                        plt.close()
                    except Exception as e:
                        self.logger.error(f"无法加载图表 {fig_path}: {str(e)}")
                
                # 添加统计表格 - 显式使用中文字体
                stats_data = [
                    ["总素材数量", len(df)],
                    ["未使用素材", f"{unused_clips} ({unused_clips/len(df)*100:.1f}%)"],
                    ["完全使用素材", f"{fully_used_clips} ({fully_used_clips/len(df)*100:.1f}%)"],
                    ["平均使用次数", f"{df['使用次数'].mean():.2f}"],
                    ["最短时长", f"{min_duration:.2f}秒"],
                    ["最长时长", f"{max_duration:.2f}秒"],
                    ["平均时长", f"{avg_duration:.2f}秒"],
                    ["中位时长", f"{median_duration:.2f}秒"],
                    ["使用次数-时长相关性", f"{correlation:.2f}"]
                ]
                
                plt.figure(figsize=(11, 8.5))
                table = plt.table(cellText=stats_data,
                        colLabels=['指标', '值'],
                        loc='center',
                        cellLoc='center',
                        colColours=['#f3f3f3']*2)
                
                # 设置表格中的字体
                for key, cell in table.get_celld().items():
                    cell.set_height(0.08)  # 调整行高确保中文显示
                
                plt.axis('off')
                pdf.savefig(bbox_inches='tight', pad_inches=0.1)
                plt.close()
            
            self.logger.info(f"生成素材分析报告: {report_path}")
            
            return report_path
        except Exception as e:
            self.logger.error(f"素材分析失败: {str(e)}", exc_info=True)
            return None
    
    def generate_and_render(self):
        """生成并渲染视频组合（主入口）"""
        # 记录程序开始时间
        program_start_time = time.time()
        
        # 创建输出目录
        os.makedirs(self.output_folder, exist_ok=True)
        self.logger.info(f"创建输出目录: {self.output_folder}")
        
        # 计算理论最大组合数
        max_combinations = self._calculate_max_combinations()
        self.logger.info(f"理论最大组合数量: {max_combinations:,}")
        self.logger.info(f"实际将生成最多 {self.max_videos} 个视频, 每个素材最多使用 {self.max_usage} 次")
        
        # 首先生成所有组合方案
        combinations = []
        while len(combinations) < self.max_videos:
            combo = self._generate_video_combination()
            if combo is None:
                self.logger.warning("无法生成更多组合，提前终止")
                break
            combinations.append(combo)
        
        self.logger.info(f"成功生成 {len(combinations)} 个组合方案，开始渲染...")
        
        # 使用线程池并发渲染视频
        video_records = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(48, os.cpu_count() or 2)) as executor:
            # 提交所有渲染任务
            future_to_index = {
                executor.submit(self._render_video, combo, index, program_start_time): index
                for index, combo in enumerate(combinations, 1)
            }
            
            # 处理完成的任务
            for future in concurrent.futures.as_completed(future_to_index):
                index = future_to_index[future]
                try:
                    result = future.result()
                    video_records.append(result)
                    if result['渲染状态'] == '成功':
                        self.logger.info(f"完成渲染任务 #{index}: {result['视频路径']}")
                    else:
                        self.logger.warning(f"渲染任务 #{index} 失败: {result.get('错误信息', '未知错误')}")
                except Exception as e:
                    self.logger.error(f"渲染任务 #{index} 处理异常: {str(e)}", exc_info=True)
                    video_records.append({
                        '视频ID': f"combo_{index:03d}",
                        '视频名称': f"combo_{index:03d}.mp4",
                        '视频路径': '',
                        '视频时长(秒)': 0,
                        '渲染状态': '异常',
                        '渲染耗时(秒)': 0,
                        '使用素材列表': '',
                        '素材ID列表': '',
                        '素材数量': 0,
                        '单个处理时长(秒)': 0,
                        '总处理时长(秒)': 0,
                        '错误信息': str(e)
                    })
        
        # 更新素材使用计数
        for clip in self.clip_records:
            clip_path = clip['素材路径']
            if clip_path in self.usage_counter:
                clip['使用次数'] = self.usage_counter[clip_path]
        
        # 生成Excel报告
        excel_path = self._create_excel_report(video_records)
        
        # 创建素材使用报告
        clip_report_path = self._create_clip_report()
        
        # 分析素材使用情况和时长分布
        analysis_report_path = self.analyze_and_visualize_clips()
        
        # 计算程序总耗时
        program_end_time = time.time()
        total_program_time = program_end_time - program_start_time
        
        # 打印总结
        success_count = sum(1 for v in video_records if v['渲染状态'] == '成功')
        failure_count = len(video_records) - success_count
        
        self.logger.info("\n" + "=" * 80)
        self.logger.info(f"视频生成完成! 共生成 {len(video_records)} 个视频")
        self.logger.info(f"成功: {success_count} 个, 失败: {failure_count} 个")
        self.logger.info(f"程序总耗时: {total_program_time:.2f} 秒")
        
        if excel_path:
            self.logger.info(f"视频生产报告已生成: {excel_path}")
        
        if clip_report_path:
            self.logger.info(f"素材使用报告已生成: {clip_report_path}")
        
        if analysis_report_path:
            self.logger.info(f"素材分析报告已生成: {analysis_report_path}")
        
        self.logger.info(f"素材使用统计:")
        for clip in self.clip_records:
            self.logger.info(f"  {clip['素材名称']}: 使用 {clip['使用次数']}/{self.max_usage} 次")
        
        self.logger.info("=" * 80)
        
        # 返回所有视频记录
        return video_records

def run_video_processor(params: dict):
    """
    视频处理任务入口（切割+混剪一体化），可被 Worker 调用。
    
    参数说明：
    - source_video_dir: 原始视频文件夹路径
    - clips_temp_dir: 临时片段存储目录
    - output_folder: 最终混剪视频输出目录
    - scene_threshold: 场景检测敏感度 (默认30.0)
    - enable_cutting: 是否启用视频切割 (默认True)
    - 其他混剪相关参数...
    """
    job = get_current_job()
    lock_key = "lock:video_processor"
    lock_timeout = 60 * 60 * 2  # 2小时

    try:
        # 初始化 Redis 连接
        redis_conn = Redis(
            host='localhost',
            port=6379,
            db=0,
            password='rkwork',
            socket_connect_timeout=5,
            socket_timeout=5,
            decode_responses=True,
        )

        # 测试连接
        if not redis_conn.ping():
            return {
                "status": "error",
                "reason": "无法连接 Redis",
                "job_id": job.id if job else None
            }

    except (AuthenticationError, ConnectionError, TimeoutError) as e:
        return {
            "status": "error",
            "reason": f"Redis 连接失败: {str(e)}",
            "job_id": job.id if job else None
        }

    # 获取分布式锁，避免同一时间执行多个视频任务
    lock = redis_conn.lock(lock_key, timeout=lock_timeout, blocking=False)
    if not lock.acquire(blocking=False):
        return {
            "status": "skipped",
            "reason": "已有视频处理任务在执行",
            "job_id": job.id if job else None
        }

    try:
        
        
        # === 切割相关参数 ===
        source_video_dir = params.get("input_folder", "data/videos")  # 原始视频目录
        clips_temp_dir = params.get("clips_temp_dir", "temp/clips")       # 临时片段目录
        scene_threshold = params.get("scene_threshold", 30.0)             # 场景检测敏感度
        enable_cutting = params.get("enable_cutting", True)               # 是否启用切割
        max_concurrent_videos = params.get("max_concurrent_videos", max(1, cpu_count() // 2))
        
        # === 混剪相关参数 ===
        output_folder = params.get("output_folder", "./rkwork/output_videos")
        min_duration = params.get("min_duration", 20)
        max_duration = params.get("max_duration", 45)
        max_clips_per_video = params.get("max_clips_per_video", 15)
        max_videos = params.get("max_videos", 5)
        max_usage = params.get("max_usage", 15)
        ttf_path = params.get("ttf_path", "fonts/汉仪晓波花月圆W.ttf")

        # 设置日志
        logger = setup_logger(output_folder)
        
        # === 步骤1: 视频切割（如果启用） ===
        if enable_cutting:
            logger.info("=" * 80)
            logger.info("开始执行视频切割任务")
            logger.info("=" * 80)
            
            # 创建视频切割器
            cutter = VideoSceneCutter(logger=logger)
            
            # 执行视频切割
            cutting_success = cutter.cut_videos_to_clips(
                video_dir=source_video_dir,
                output_root=clips_temp_dir,
                threshold=scene_threshold,
                max_concurrent_videos=max_concurrent_videos
            )
            
            if not cutting_success:
                return {
                    "status": "error",
                    "reason": "视频切割阶段失败",
                    "job_id": job.id if job else None
                }
            
            logger.info("视频切割阶段完成")
            
            # 使用切割后的片段作为混剪素材输入
            input_folder = clips_temp_dir
        else:
            logger.info("跳过视频切割，直接使用现有素材进行混剪")
            input_folder = params.get("input_folder", clips_temp_dir)
        
        # === 步骤2: 视频混剪 ===
        logger.info("=" * 80)
        logger.info("开始执行视频混剪任务")
        logger.info("=" * 80)
        
        # 创建 VideoCombinator 实例
        combinator = VideoCombinator(
            input_folder=input_folder,
            output_folder=output_folder,
            min_duration=min_duration,
            max_duration=max_duration,
            max_clips_per_video=max_clips_per_video,
            max_videos=max_videos,
            max_usage=max_usage,
            ttf_path=ttf_path,
            logger=logger
        )

        # 执行混剪
        video_records = combinator.generate_and_render()

        # 打印最终摘要
        logger.info("\n" + "=" * 80)
        logger.info("视频处理任务完成摘要:")
        if enable_cutting:
            logger.info(f"success 视频切割: 从 {source_video_dir} 切割片段到 {clips_temp_dir}")
        logger.info(f"success 视频混剪: 生成 {len(video_records)} 个混剪视频到 {output_folder}")
        
        if video_records:
            success_count = sum(1 for record in video_records if record['渲染状态'] == '成功')
            logger.info(f"success 成功生成视频: {success_count}/{len(video_records)}")
            
            for record in video_records:
                status = "success" if record['渲染状态'] == '成功' else "✗"
                logger.info(
                    f"  {status} {record['视频ID']}: "
                    f"{record['视频时长(秒)']:.1f}s, "
                    f"素材: {record['素材数量']}个, "
                    f"耗时: {record['渲染耗时(秒)']:.2f}s"
                )
        logger.info("=" * 80)

        return {
            "status": "success",
            "video_records": video_records,
            "cutting_enabled": enable_cutting,
            "source_dir": source_video_dir if enable_cutting else input_folder,
            "clips_dir": clips_temp_dir if enable_cutting else None,
            "output_dir": output_folder,
            "job_id": job.id if job else None
        }
        
    except Exception as e:
        logger.error(f"视频处理任务异常: {str(e)}", exc_info=True)
        return {
            "status": "error",
            "reason": f"任务执行异常: {str(e)}",
            "job_id": job.id if job else None
        }
    finally:
        # 释放锁
        lock.release()
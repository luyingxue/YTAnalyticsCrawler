from multiprocessing import Process, Pool, cpu_count, Value
import time
import signal
import sys
from crawler import YoutubeCrawler
from channel_crawler import ChannelCrawler
from log_manager import LogManager
import configparser
import ctypes
from src.services import ChannelService, VideoService, KeywordService

# 使用多进程共享变量
should_exit = Value(ctypes.c_bool, False)

def signal_handler(signum, frame):
    """处理Ctrl+C信号"""
    logger = LogManager().get_logger()
    logger.info("\n接收到终止信号，正在安全退出...")
    
    # 给进程一些时间来清理资源
    time.sleep(2)
    
    # 设置共享的退出标志
    should_exit.value = True

def channel_worker(worker_id=None):
    """频道爬取工作进程"""
    try:
        logger = LogManager().get_logger()
        logger.info(f"启动频道爬取进程 {worker_id}")
        
        crawler = ChannelCrawler(worker_id=worker_id)
        crawler.setup()
        
        # 直接使用ChannelService
        channel_service = ChannelService()
        
        while not should_exit.value:
            try:
                # 获取未爬取的频道
                channel = channel_service.get_uncrawled_channel()
                
                if not channel:
                    logger.info(f"[进程 {worker_id}] 所有频道今天都已经爬取过，等待5分钟后继续...")
                    time.sleep(300)
                    continue
                
                logger.info(
                    f"[进程 {worker_id}] 开始爬取频道: "
                    f"channel_id={channel['channel_id']}, "
                    f"is_benchmark={channel['is_benchmark']}, "
                    f"url={channel['url']}"
                )
                
                # 爬取频道信息
                channel_info = crawler.crawl_channel(channel['url'])
                if channel_info:
                    # 确保channel_id正确
                    channel_info['channel_id'] = channel['channel_id']
                    # 保存到数据库
                    try:
                        channel_service.insert_channel_crawl(channel_info)
                        logger.info(f"[进程 {worker_id}] 成功保存频道数据: {channel['channel_id']}")
                    except Exception as e:
                        if "Duplicate entry" not in str(e):
                            raise
                        logger.info(f"[进程 {worker_id}] 频道数据已存在: {channel['channel_id']}")
                else:
                    logger.error(f"[进程 {worker_id}] 爬取频道失败: {channel['channel_id']}")
                    
                    # 删除不存在的频道记录
                    try:
                        logger.info(f"[进程 {worker_id}] 正在删除不存在的频道记录: {channel['channel_id']}")
                        deleted = channel_service.delete_channel(channel['channel_id'])
                        if deleted:
                            logger.info(f"[进程 {worker_id}] 成功删除不存在的频道记录: {channel['channel_id']}")
                        else:
                            logger.info(f"[进程 {worker_id}] 删除不存在的频道记录失败: {channel['channel_id']}")
                    except Exception as delete_err:
                        logger.error(f"[进程 {worker_id}] 删除不存在频道记录时出错: {str(delete_err)}")
                
                # 等待一段时间再处理下一个频道
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"[进程 {worker_id}] 处理频道时出错: {str(e)}")
                time.sleep(60)
                continue
                
    except Exception as e:
        logger.error(f"[进程 {worker_id}] 频道爬取进程出错: {str(e)}")
    finally:
        logger.info(f"[进程 {worker_id}] 正在清理资源...")
        crawler.cleanup()
        logger.info(f"[进程 {worker_id}] 进程结束")

def main():
    """主函数"""
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    logger = LogManager().get_logger()
    channel_procs = []
    
    try:
        logger.info("程序启动，按Ctrl+C可以安全退出")
        
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read('config.ini', encoding='utf-8')
        
        # 读取开关配置
        enable_video = int(config['crawler'].get('enable_video_crawler', 0))
        enable_channel = int(config['crawler'].get('enable_channel_crawler', 0))
        
        if not enable_video and not enable_channel:
            logger.error("视频爬取和频道爬取都已关闭，程序退出")
            return
            
        # 视频爬取进程配置
        if enable_video:
            config_processes = int(config['crawler'].get('num_processes', 1))
            retry_wait = int(config['crawler'].get('retry_wait', 300))
            max_processes = cpu_count() - 1
            num_processes = min(config_processes, max_processes)
            logger.info(f"视频爬取已启用，进程数: {num_processes}")
        else:
            logger.info("视频爬取已关闭")
            
        # 频道爬取进程配置
        if enable_channel:
            channel_processes = int(config['crawler'].get('channel_processes', 1))
            logger.info(f"频道爬取已启用，进程数: {channel_processes}")
            
            # 启动频道爬取进程
            for i in range(channel_processes):
                proc = Process(target=channel_worker, kwargs={'worker_id': i})
                proc.start()
                channel_procs.append(proc)
                logger.info(f"频道爬取进程 {i+1} 已启动")
        else:
            logger.info("频道爬取已关闭")
        
        # 等待所有进程结束
        while any(p.is_alive() for p in channel_procs):
            if should_exit.value:
                logger.info("等待进程清理资源...")
                # 给进程一些时间来清理
                time.sleep(5)
                break
            time.sleep(1)
            
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
    finally:
        logger.info("程序结束")
        # 确保所有进程都已经结束
        for proc in channel_procs:
            if proc.is_alive():
                proc.join(timeout=10)  # 最多等待10秒
                if proc.is_alive():
                    proc.terminate()

if __name__ == "__main__":
    main() 
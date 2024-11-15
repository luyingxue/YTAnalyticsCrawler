from multiprocessing import Pool, cpu_count
import time
import signal
import sys
from crawler import YoutubeCrawler
from db_manager import DBManager
from log_manager import LogManager
import configparser

def crawl_worker(url_data):
    """工作进程的处理函数"""
    try:
        # 获取日志记录器
        logger = LogManager().get_logger()
        logger.info(f"开始处理URL: {url_data['url']}")
        
        # 创建爬虫实例
        crawler = YoutubeCrawler(worker_id=url_data['id'])
        
        # 初始化爬虫
        crawler.setup()
        
        # 处理URL
        crawler.process_url(url_data)
        
        logger.info(f"URL处理完成: {url_data['url']}")
        return True
        
    except Exception as e:
        logger.error(f"处理URL时出错: {url_data['url']} - {str(e)}")
        return False
        
    finally:
        # 清理资源
        crawler.cleanup()

def signal_handler(signum, frame):
    """处理Ctrl+C信号"""
    logger = LogManager().get_logger()
    logger.info("\n接收到终止信号，正在安全退出...")
    sys.exit(0)

def main():
    """主函数"""
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 获取日志记录器
    logger = LogManager().get_logger()
    
    try:
        logger.info("程序启动，按Ctrl+C可以安全退出")
        
        # 读取配置文件和设置进程数
        config = configparser.ConfigParser()
        config.read('config.ini')
        config_processes = int(config['crawler'].get('num_processes', 1))
        retry_wait = int(config['crawler'].get('retry_wait', 300))
        max_processes = cpu_count() - 1
        num_processes = min(config_processes, max_processes)
        
        logger.info(f"配置进程数: {config_processes}, CPU核心数-1: {max_processes}")
        logger.info(f"将使用 {num_processes} 个进程")
        
        while True:  # 永久循环
            try:
                # 获取活跃的搜索关键词
                db = DBManager()
                try:
                    # 一次性获取所有需要的关键词
                    keywords = db.get_active_keywords(num_processes)
                    
                    if not keywords:
                        logger.info("所有关键词今天都已经抓取过，等待5分钟后继续...")
                        time.sleep(300)  # 等待5分钟
                        continue  # 继续循环
                    
                    # 创建进程池
                    with Pool(processes=len(keywords)) as pool:
                        # 分配任务给工作进程
                        results = pool.map(crawl_worker, keywords)
                        
                        # 检查结果
                        success = sum(1 for r in results if r)
                        logger.info(f"本轮处理完成: {success}/{len(keywords)} 个关键词成功")
                    
                finally:
                    db.disconnect()  # 确保数据库连接被关闭
                
                # 等待一段时间再开始下一轮
                time.sleep(60)  # 等待60秒
                
            except Exception as e:
                logger.error(f"处理一轮关键词时出错: {str(e)}")
                logger.info(f"等待 {retry_wait} 秒后重试...")
                time.sleep(retry_wait)
                
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
    finally:
        logger.info("程序结束")

if __name__ == "__main__":
    main() 
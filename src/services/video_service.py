from ..models import VideoModel
from src.utils.logger import Logger
from datetime import datetime
import logging

class VideoService:
    """视频服务类，处理视频相关的业务逻辑"""
    
    def __init__(self):
        """初始化视频服务"""
        self.model = VideoModel()
        self.logger = Logger().get_logger('VideoService')
        
    def log(self, message, level='INFO'):
        """输出日志"""
        # 将字符串日志级别转换为对应的整数常量
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        level_int = level_map.get(level, logging.INFO)
        self.logger.log(level_int, message)
        
    def save_video_data(self, video_data):
        """保存视频数据到数据库"""
        # 这里可以添加数据验证、转换等业务逻辑
        return self.model.save_video_data(video_data)
        
    def save_videos_batch(self, videos_data):
        """批量保存视频数据"""
        # 这里可以添加数据验证、转换等业务逻辑
        return self.model.save_videos_batch(videos_data)
        
    def get_uncrawled_url(self):
        """获取未爬取的视频URL"""
        try:
            # 调用存储过程获取未爬取的URL
            result = self.model.call_rpc('get_next_uncrawled_url')
            
            if result:
                # 记录日志
                self.log(f"获取到未爬取URL: {result['url']}, 是否对标: {result.get('is_benchmark')}")
                return result
            else:
                self.log("没有找到未爬取的URL")
                return None
                
        except Exception as e:
            self.log(f"获取未爬取URL时出错: {str(e)}", "ERROR")
            return None
            
    def mark_url_as_crawled(self, url):
        """标记URL为已爬取"""
        try:
            # 更新URL状态
            result = self.model.update_url_status(url, {
                'last_crawl_date': datetime.now().date().isoformat(),
                'crawl_status': 'success',
                'fail_count': 0
            })
            
            if result:
                self.log(f"成功标记URL为已爬取: {url}")
                return True
            else:
                self.log(f"标记URL为已爬取失败: {url}", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"标记URL为已爬取时出错: {str(e)}", "ERROR")
            return False
            
    def mark_url_as_failed(self, url):
        """标记URL为爬取失败"""
        try:
            # 获取当前失败次数
            url_info = self.model.get_url_info(url)
            if not url_info:
                self.log(f"获取URL信息失败: {url}", "ERROR")
                return False
                
            fail_count = url_info.get('fail_count', 0) + 1
            
            # 更新URL状态
            result = self.model.update_url_status(url, {
                'last_crawl_date': datetime.now().date().isoformat(),
                'crawl_status': 'failed',
                'fail_count': fail_count
            })
            
            if result:
                self.log(f"成功标记URL为爬取失败: {url}, 失败次数: {fail_count}")
                return True
            else:
                self.log(f"标记URL为爬取失败失败: {url}", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"标记URL为爬取失败时出错: {str(e)}", "ERROR")
            return False 
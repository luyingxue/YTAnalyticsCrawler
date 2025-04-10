from log_manager import LogManager
from src.services import ChannelService, VideoService, KeywordService

class DBManager:
    """数据库管理类"""
    
    def __init__(self):
        """初始化数据库管理器"""
        self.logger = LogManager().get_logger('DBManager')
        self.channel_service = ChannelService()
        self.video_service = VideoService()
        self.keyword_service = KeywordService()
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
        
    def insert_channel_crawl(self, channel_info):
        """插入频道爬取数据"""
        return self.channel_service.insert_channel_crawl(channel_info)
        
    def get_uncrawled_channel(self):
        """获取今天未爬取的频道"""
        return self.channel_service.get_uncrawled_channel()
            
    def save_video_data(self, video_data):
        """保存视频数据到数据库"""
        return self.video_service.save_video_data(video_data)
            
    def save_videos_batch(self, videos_data):
        """批量保存视频数据"""
        return self.video_service.save_videos_batch(videos_data)
            
    def get_uncrawled_keywords(self):
        """获取未爬取的关键词"""
        return self.keyword_service.get_uncrawled_keywords()
            
    def save_keyword_data(self, keyword_data):
        """保存关键词数据"""
        return self.keyword_service.save_keyword_data(keyword_data)
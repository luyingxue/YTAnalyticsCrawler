from ..models import VideoModel
from log_manager import LogManager

class VideoService:
    """视频服务类，处理视频相关的业务逻辑"""
    
    def __init__(self):
        """初始化视频服务"""
        self.model = VideoModel()
        self.logger = LogManager().get_logger('VideoService')
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
        
    def save_video_data(self, video_data):
        """保存视频数据到数据库"""
        # 这里可以添加数据验证、转换等业务逻辑
        return self.model.save_video_data(video_data)
        
    def save_videos_batch(self, videos_data):
        """批量保存视频数据"""
        # 这里可以添加数据验证、转换等业务逻辑
        return self.model.save_videos_batch(videos_data) 
from ..models import ChannelModel

class ChannelService:
    """频道服务类，处理频道相关的业务逻辑"""
    
    def __init__(self):
        """初始化频道服务"""
        self.model = ChannelModel()
        from src.utils import Logger
        self.logger = Logger().get_logger('ChannelService')
        
    def log(self, message, level='INFO'):
        """输出日志"""
        self.logger.log(message, level)
        
    def insert_channel_crawl(self, channel_info):
        """插入频道爬取数据"""
        # 这里可以添加数据验证、转换等业务逻辑
        return self.model.insert_channel_crawl(channel_info)
        
    def get_uncrawled_channel(self):
        """获取今天未爬取的频道"""
        return self.model.get_uncrawled_channel()
        
    def delete_channel(self, channel_id):
        """删除频道记录"""
        # 这里可以添加数据验证、转换等业务逻辑
        return self.model.delete_channel(channel_id) 
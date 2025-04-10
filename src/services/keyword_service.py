from ..models import KeywordModel
from src.utils.logger import Logger

class KeywordService:
    """关键词服务类，处理关键词相关的业务逻辑"""
    
    def __init__(self):
        """初始化关键词服务"""
        self.model = KeywordModel()
        self.logger = Logger().get_logger('KeywordService')
        
    def log(self, message, level='INFO'):
        """输出日志"""
        self.logger.log(message, level)
        
    def get_uncrawled_keywords(self):
        """获取未爬取的关键词"""
        return self.model.get_uncrawled_keywords()
        
    def save_keyword_data(self, keyword_data):
        """保存关键词数据"""
        # 这里可以添加数据验证、转换等业务逻辑
        return self.model.save_keyword_data(keyword_data) 
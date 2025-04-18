from ..models import KeywordModel
from src.utils.logger import Logger
import logging

class KeywordService:
    """关键词服务类，处理关键词相关的业务逻辑"""
    
    def __init__(self):
        """初始化关键词服务"""
        self.model = KeywordModel()
        self.logger = Logger().get_logger('KeywordService')
        
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
        
    def get_uncrawled_keywords(self):
        """获取未爬取的关键词"""
        try:
            # 调用存储过程获取未爬取的关键词
            result = self.model.call_rpc('get_next_uncrawled_keyword')
            
            if result:
                # 记录日志
                self.log(f"获取到未爬取关键词: {result['key_words']}, ID: {result['id']}")
                return result
            else:
                self.log("没有找到未爬取的关键词")
                return None
                
        except Exception as e:
            self.log(f"获取未爬取关键词时出错: {str(e)}", "ERROR")
            return None
        
    def save_keyword_data(self, keyword_data):
        """保存关键词数据"""
        # 这里可以添加数据验证、转换等业务逻辑
        return self.model.save_keyword_data(keyword_data) 
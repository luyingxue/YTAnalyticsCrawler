import os
from ..db import create_connection_pool

class BaseModel:
    """基础模型类，提供数据库连接和通用方法"""
    
    def __init__(self):
        """初始化基础模型"""
        # 获取项目根目录下的config.ini路径
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config.ini')
        self.connection_pool = create_connection_pool(config_path)
        from src.utils import Logger
        self.logger = Logger()
        
    def log(self, message, level='INFO'):
        """输出日志"""
        level_int = self.logger._get_level_int(level)
        self.logger.log(message, level_int)
        
    def execute_query(self, query, params=None, fetch=True):
        """执行SQL查询"""
        return self.connection_pool.execute_query(query, params, fetch)
        
    def execute_many(self, query, params_list):
        """批量执行SQL语句"""
        return self.connection_pool.execute_many(query, params_list)
        
    def transaction(self):
        """获取事务上下文管理器"""
        return self.connection_pool.transaction() 
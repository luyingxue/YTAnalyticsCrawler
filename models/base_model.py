import sys
import os

# 添加项目根目录到Python路径
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from db import create_connection_pool
from log_manager import LogManager

class BaseModel:
    """基础模型类，提供数据库连接和通用方法"""
    
    def __init__(self):
        """初始化基础模型"""
        # 获取项目根目录下的config.ini路径
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini')
        self.connection_pool = create_connection_pool(config_path)
        self.logger = LogManager().get_logger(self.__class__.__name__)
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
        
    def execute_query(self, query, params=None, fetch=True):
        """执行SQL查询"""
        return self.connection_pool.execute_query(query, params, fetch)
        
    def execute_many(self, query, params_list):
        """批量执行SQL语句"""
        return self.connection_pool.execute_many(query, params_list)
        
    def transaction(self):
        """获取事务上下文管理器"""
        return self.connection_pool.transaction() 
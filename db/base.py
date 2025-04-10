from mysql.connector import Error as MySQLError
from .exceptions import DBConnectionError, DBQueryError
from log_manager import LogManager

class DBBase:
    """数据库操作基类"""
    
    def __init__(self, config):
        """初始化数据库配置"""
        self.config = config
        self.logger = LogManager().get_logger(self.__class__.__name__)
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
        
    def execute_query(self, query, params=None, fetch=True):
        """执行SQL查询"""
        raise NotImplementedError("子类必须实现execute_query方法")
        
    def execute_many(self, query, params_list):
        """批量执行SQL语句"""
        raise NotImplementedError("子类必须实现execute_many方法")
        
    def get_connection(self):
        """获取数据库连接"""
        raise NotImplementedError("子类必须实现get_connection方法")
        
    def close_connection(self, connection):
        """关闭数据库连接"""
        if connection and connection.is_connected():
            connection.close()
            self.log("数据库连接已关闭") 
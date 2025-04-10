import mysql.connector
from mysql.connector import Error
import configparser
from log_manager import LogManager

class DatabaseConnection:
    """数据库连接管理类，处理与MySQL的连接"""
    
    def __init__(self, config_path='config.ini'):
        """初始化数据库连接配置"""
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read(config_path)
        
        self.connection_config = {
            'host': config['database']['host'],
            'database': config['database']['database'],
            'user': config['database']['user'],
            'password': config['database']['password']
        }
        
        self.connection = None
        self.logger = LogManager().get_logger('DatabaseConnection')
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
        
    def connect(self):
        """建立数据库连接"""
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**self.connection_config)
                self.log("数据库连接成功")
        except Error as e:
            self.log(f"数据库连接错误: {str(e)}", 'ERROR')
            raise
            
    def disconnect(self):
        """关闭数据库连接"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            self.log("数据库连接已关闭")
            
    def get_connection(self):
        """获取数据库连接"""
        if self.connection is None or not self.connection.is_connected():
            self.connect()
        return self.connection
        
    def execute_query(self, query, params=None, fetch=True):
        """执行SQL查询"""
        try:
            self.connect()
            cursor = self.connection.cursor(dictionary=True)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            if fetch:
                result = cursor.fetchall()
            else:
                result = cursor.rowcount
                self.connection.commit()
                
            cursor.close()
            return result
            
        except Error as e:
            self.log(f"执行查询错误: {str(e)}", 'ERROR')
            if self.connection:
                self.connection.rollback()
            raise
            
    def execute_many(self, query, params_list):
        """批量执行SQL语句"""
        try:
            self.connect()
            cursor = self.connection.cursor()
            
            cursor.executemany(query, params_list)
            affected_rows = cursor.rowcount
            
            self.connection.commit()
            cursor.close()
            
            return affected_rows
            
        except Error as e:
            self.log(f"批量执行错误: {str(e)}", 'ERROR')
            if self.connection:
                self.connection.rollback() 
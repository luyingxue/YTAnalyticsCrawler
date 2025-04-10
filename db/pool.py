import mysql.connector
from mysql.connector import Error as MySQLError
from log_manager import LogManager
from contextlib import contextmanager
from .exceptions import DBConnectionError, DBQueryError, DBPoolError

class ConnectionPool:
    """数据库连接管理类
    
    提供数据库连接池功能，支持：
    - 连接池管理
    - 事务支持
    - 查询执行
    - 批量操作
    """
    
    def __init__(self, config, pool_size=5, pool_name="mypool"):
        """初始化数据库连接池
        
        Args:
            config (dict): 数据库配置信息
            pool_size (int): 连接池大小，默认5
            pool_name (str): 连接池名称，默认"mypool"
        """
        self.config = config
        self.pool_size = pool_size
        self.pool_name = pool_name
        self.pool = None
        self.logger = LogManager().get_logger('ConnectionPool')
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
        
    def create_pool(self):
        """创建数据库连接池"""
        try:
            if self.pool is None:
                self.pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name=self.pool_name,
                    pool_size=self.pool_size,
                    **self.config
                )
                self.log(f"首次创建连接池成功，大小: {self.pool_size}")
            else:
                self.log("复用已存在的连接池")
        except MySQLError as e:
            self.log(f"创建连接池错误: {str(e)}", 'ERROR')
            raise DBPoolError(f"创建连接池错误: {str(e)}")
            
    def get_connection(self):
        """从连接池获取数据库连接"""
        if self.pool is None:
            self.create_pool()
            
        try:
            connection = self.pool.get_connection()
            self.log("从连接池获取连接成功")
            return connection
        except MySQLError as e:
            self.log(f"获取连接错误: {str(e)}", 'ERROR')
            raise DBConnectionError(f"获取连接错误: {str(e)}")
            
    def close_connection(self, connection):
        """关闭数据库连接"""
        if connection and connection.is_connected():
            connection.close()
            self.log("数据库连接已关闭")
            
    @contextmanager
    def transaction(self):
        """事务上下文管理器
        
        用法:
            with pool.transaction() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM table")
        """
        connection = None
        try:
            connection = self.get_connection()
            yield connection
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            self.log(f"事务执行错误: {str(e)}", 'ERROR')
            raise DBQueryError(f"事务执行错误: {str(e)}")
        finally:
            if connection:
                self.close_connection(connection)
                
    def execute_query(self, query, params=None, fetch=True):
        """执行SQL查询
        
        Args:
            query (str): SQL查询语句
            params (tuple/dict): 查询参数
            fetch (bool): 是否获取结果
            
        Returns:
            查询结果或影响行数
        """
        with self.transaction() as connection:
            cursor = connection.cursor(dictionary=True)
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
                
            if fetch:
                result = cursor.fetchall()
            else:
                result = cursor.rowcount
                
            cursor.close()
            return result
            
    def execute_many(self, query, params_list):
        """批量执行SQL语句
        
        Args:
            query (str): SQL语句
            params_list (list): 参数列表
            
        Returns:
            影响的行数
        """
        with self.transaction() as connection:
            cursor = connection.cursor()
            
            cursor.executemany(query, params_list)
            affected_rows = cursor.rowcount
            
            cursor.close()
            return affected_rows 
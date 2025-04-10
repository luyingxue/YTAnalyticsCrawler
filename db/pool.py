import mysql.connector
from mysql.connector import Error
import configparser
from log_manager import LogManager
import random
import time
from contextlib import contextmanager

class ConnectionPool:
    """数据库连接池管理类"""
    
    def __init__(self, config_path='config.ini', pool_size=5, pool_name="mypool"):
        """初始化连接池"""
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read(config_path)
        
        self.connection_config = {
            'host': config['database']['host'],
            'database': config['database']['database'],
            'user': config['database']['user'],
            'password': config['database']['password']
        }
        
        self.pool_size = pool_size
        self.pool_name = pool_name
        self.pool = None
        self.logger = LogManager().get_logger('ConnectionPool')
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
        
    def create_pool(self):
        """创建连接池"""
        try:
            if self.pool is None:
                self.pool = mysql.connector.pooling.MySQLConnectionPool(
                    pool_name=self.pool_name,
                    pool_size=self.pool_size,
                    **self.connection_config
                )
                self.log(f"连接池创建成功，大小: {self.pool_size}")
        except Error as e:
            self.log(f"创建连接池错误: {str(e)}", 'ERROR')
            raise
            
    def get_connection(self):
        """从连接池获取连接"""
        if self.pool is None:
            self.create_pool()
            
        try:
            connection = self.pool.get_connection()
            self.log("从连接池获取连接成功")
            return connection
        except Error as e:
            self.log(f"获取连接错误: {str(e)}", 'ERROR')
            raise
            
    @contextmanager
    def transaction(self):
        """事务上下文管理器"""
        connection = None
        try:
            connection = self.get_connection()
            yield connection
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            self.log(f"事务执行错误: {str(e)}", 'ERROR')
            raise
        finally:
            if connection:
                connection.close()
                
    def execute_query(self, query, params=None, fetch=True):
        """执行SQL查询"""
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
        """批量执行SQL语句"""
        with self.transaction() as connection:
            cursor = connection.cursor()
            
            cursor.executemany(query, params_list)
            affected_rows = cursor.rowcount
            
            cursor.close()
            return affected_rows
            
    def get_uncrawled_channel(self):
        """获取今天未爬取的频道，使用串行事务，带重试机制"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                with self.transaction() as connection:
                    cursor = connection.cursor(dictionary=True)
                    
                    try:
                        # 开始事务
                        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                        cursor.execute("START TRANSACTION")
                        
                        # 获取一条未爬取的频道
                        query = """
                            SELECT channel_id, is_benchmark, last_crawl_date
                            FROM channel_base
                            WHERE 
                                (last_crawl_date IS NULL OR last_crawl_date != CURRENT_DATE)
                                AND is_blacklist = 0
                            ORDER BY 
                                is_benchmark DESC,
                                CASE 
                                    WHEN last_crawl_date IS NULL THEN 1 
                                    ELSE 0 
                                END DESC,
                                last_crawl_date ASC
                            LIMIT 1
                            FOR UPDATE
                        """
                        
                        cursor.execute(query)
                        result = cursor.fetchone()
                        
                        if result:
                            # 立即更新last_crawl_date
                            update_query = """
                                UPDATE channel_base 
                                SET last_crawl_date = CURRENT_DATE
                                WHERE channel_id = %s
                            """
                            cursor.execute(update_query, (result['channel_id'],))
                            
                            # 构建URL
                            result['url'] = f"https://www.youtube.com/channel/{result['channel_id']}/shorts"
                            self.log(f"获取到未爬取频道: channel_id={result['channel_id']}, is_benchmark={result['is_benchmark']}, last_crawl={result['last_crawl_date']}")
                            
                            return result
                        else:
                            self.log("没有找到未爬取的频道")
                            return None
                            
                    except Error as e:
                        raise
                        
            except Error as e:
                if "Deadlock found" in str(e):
                    retry_count += 1
                    self.log(f"发生死锁，正在重试 ({retry_count}/{max_retries})")
                    time.sleep(random.uniform(0.1, 0.5))  # 随机延迟，避免同时重试
                    continue
                self.log(f"获取未爬取频道时出错: {str(e)}", 'ERROR')
                raise
                
        self.log(f"达到最大重试次数 ({max_retries})，放弃获取")
        return None 
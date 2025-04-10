from .connection import DatabaseConnection
from .pool import ConnectionPool
from .exceptions import DBError, DBConnectionError, DBQueryError, DBPoolError
import configparser

__all__ = [
    'DatabaseConnection', 
    'ConnectionPool', 
    'create_db_connection', 
    'create_connection_pool', 
    'get_db_config',
    'DBError',
    'DBConnectionError',
    'DBQueryError',
    'DBPoolError'
]

def get_db_config(config_path='config.ini'):
    """获取数据库配置"""
    config = configparser.ConfigParser()
    config.read(config_path)
    return {
        'host': config['database']['host'],
        'database': config['database']['database'],
        'user': config['database']['user'],
        'password': config['database']['password']
    }

def create_db_connection(config_path='config.ini'):
    """创建数据库连接"""
    return DatabaseConnection(config_path)

def create_connection_pool(config_path='config.ini', pool_size=5, pool_name="mypool"):
    """创建连接池"""
    return ConnectionPool(config_path, pool_size, pool_name) 
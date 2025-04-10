from .pool import ConnectionPool
from .exceptions import DBError, DBConnectionError, DBQueryError, DBPoolError
import configparser

__all__ = [
    'ConnectionPool', 
    'create_connection_pool', 
    'get_db_config',
    'DBError',
    'DBConnectionError',
    'DBQueryError',
    'DBPoolError'
]

# 全局连接池实例
_global_pool = None

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

def create_connection_pool(config_path='config.ini', pool_size=5, pool_name="mypool"):
    """创建数据库连接池（单例模式）"""
    global _global_pool
    if _global_pool is None:
        config = get_db_config(config_path)
        _global_pool = ConnectionPool(config, pool_size, pool_name)
    return _global_pool 
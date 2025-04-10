from .pool import ConnectionPool
from .exceptions import DBError, DBConnectionError, DBQueryError, DBPoolError
from .config import DBConfig
import os

__all__ = [
    'ConnectionPool', 
    'create_connection_pool', 
    'DBConfig',
    'DBError',
    'DBConnectionError',
    'DBQueryError',
    'DBPoolError'
]

# 全局连接池实例
_global_pool = None

def create_connection_pool(config_path: str = None, pool_size: int = 5, pool_name: str = "mypool") -> ConnectionPool:
    """创建数据库连接池（单例模式）
    
    Args:
        config_path: 配置文件路径，默认None（使用当前目录下的config.ini）
        pool_size: 连接池大小，默认5
        pool_name: 连接池名称，默认"mypool"
        
    Returns:
        ConnectionPool实例
    """
    global _global_pool
    if _global_pool is None:
        # 如果未指定配置文件路径，使用当前目录下的config.ini
        if config_path is None:
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config.ini')
            
        config = DBConfig.get_config(config_path)
        _global_pool = ConnectionPool(config, pool_size, pool_name)
    return _global_pool 
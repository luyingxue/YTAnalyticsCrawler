import os
import configparser
from typing import Dict, Any

class DBConfig:
    """数据库配置管理类"""
    
    DEFAULT_CONFIG = {
        'host': 'localhost',
        'database': 'youtube_data',
        'user': 'root',
        'password': '',
        'port': 3306
    }
    
    @classmethod
    def from_file(cls, config_path: str = 'config.ini') -> Dict[str, Any]:
        """从文件加载配置
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            数据库配置字典
        """
        if not os.path.exists(config_path):
            print(f"配置文件不存在: {config_path}")
            return cls.DEFAULT_CONFIG
            
        config = configparser.ConfigParser()
        config.read(config_path)
        
        if 'database' not in config:
            print(f"配置文件中没有database部分: {config_path}")
            return cls.DEFAULT_CONFIG
            
        db_config = config['database']
        result = {
            'host': db_config.get('host', cls.DEFAULT_CONFIG['host']),
            'database': db_config.get('database', cls.DEFAULT_CONFIG['database']),
            'user': db_config.get('user', cls.DEFAULT_CONFIG['user']),
            'password': db_config.get('password', cls.DEFAULT_CONFIG['password']),
            'port': db_config.getint('port', cls.DEFAULT_CONFIG['port'])
        }
        print(f"从配置文件读取的数据库配置: {result}")
        return result
        
    @classmethod
    def from_env(cls) -> Dict[str, Any]:
        """从环境变量加载配置
        
        Returns:
            数据库配置字典
        """
        result = {
            'host': os.getenv('DB_HOST', cls.DEFAULT_CONFIG['host']),
            'database': os.getenv('DB_NAME', cls.DEFAULT_CONFIG['database']),
            'user': os.getenv('DB_USER', cls.DEFAULT_CONFIG['user']),
            'password': os.getenv('DB_PASSWORD', cls.DEFAULT_CONFIG['password']),
            'port': int(os.getenv('DB_PORT', cls.DEFAULT_CONFIG['port']))
        }
        print(f"从环境变量读取的数据库配置: {result}")
        return result
        
    @classmethod
    def get_config(cls, config_path: str = None) -> Dict[str, Any]:
        """获取数据库配置
        
        优先使用环境变量，其次使用配置文件
        
        Args:
            config_path: 配置文件路径，默认None
            
        Returns:
            数据库配置字典
        """
        # 优先使用配置文件
        if config_path:
            file_config = cls.from_file(config_path)
            if file_config != cls.DEFAULT_CONFIG:
                return file_config
            
        # 其次使用环境变量
        env_config = cls.from_env()
        if any(env_config.values()):
            return env_config
            
        # 最后使用默认配置
        print(f"使用默认数据库配置: {cls.DEFAULT_CONFIG}")
        return cls.DEFAULT_CONFIG 
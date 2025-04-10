import logging
import os
from datetime import datetime

class Logger:
    """日志记录器类，处理日志配置和记录相关操作"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._setup_logging()
            cls._instance._logger = logging.getLogger()
        return cls._instance
    
    def _setup_logging(self):
        """设置日志配置"""
        # 创建logs目录
        if not os.path.exists('logs'):
            os.makedirs('logs')
            
        # 获取当前日期作为文件名
        current_date = datetime.now().strftime('%Y%m%d')
        
        # 设置日志格式
        formatter = logging.Formatter(
            '%(asctime)s - %(processName)s - %(threadName)s - %(levelname)s - %(message)s'
        )
        
        # 设置文件处理器
        file_handler = logging.FileHandler(
            filename=f'logs/crawler_{current_date}.log',
            encoding='utf-8'
        )
        file_handler.setFormatter(formatter)
        
        # 设置控制台处理器
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        
        # 配置根日志记录器
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        root_logger.addHandler(file_handler)
        root_logger.addHandler(console_handler)
        
        # 设置特定模块的日志级别
        logging.getLogger('selenium').setLevel(logging.WARNING)
        logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    @staticmethod
    def get_logger(name=None):
        """获取日志记录器"""
        return logging.getLogger(name)
    
    def _get_level_int(self, level):
        """将字符串日志级别转换为整数级别"""
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        return level_map.get(level, logging.INFO)
    
    def log(self, message, level='INFO', worker_id=None):
        """记录日志（实例方法）"""
        # 添加worker_id前缀
        if worker_id is not None:
            message = f"[Worker {worker_id}] {message}"
            
        # 获取对应的整数级别
        int_level = self._get_level_int(level)
        self._logger.log(int_level, message)
    
    @classmethod
    def log_static(cls, level, message, worker_id=None):
        """记录日志（静态方法）"""
        # 添加worker_id前缀
        if worker_id is not None:
            message = f"[Worker {worker_id}] {message}"
            
        # 获取对应的整数级别
        int_level = cls()._get_level_int(level)
        cls()._logger.log(int_level, message) 
import logging
import os
from datetime import datetime

class LogManager:
    """日志管理类，处理所有日志相关操作"""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(LogManager, cls).__new__(cls)
            cls._instance._setup_logging()
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
        
    @staticmethod
    def log(level, message, worker_id=None):
        """记录日志"""
        logger = LogManager.get_logger()
        
        # 添加worker_id前缀
        if worker_id is not None:
            message = f"[Worker {worker_id}] {message}"
            
        if level == 'DEBUG':
            logger.debug(message)
        elif level == 'INFO':
            logger.info(message)
        elif level == 'WARNING':
            logger.warning(message)
        elif level == 'ERROR':
            logger.error(message)
        elif level == 'CRITICAL':
            logger.critical(message) 
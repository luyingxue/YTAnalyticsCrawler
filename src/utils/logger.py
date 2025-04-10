from log_manager import LogManager

class Logger:
    """日志处理类"""
    
    def __init__(self):
        self.logger = LogManager().get_logger('Utils')
    
    def log(self, message: str, level: str = 'INFO') -> None:
        """
        输出日志
        Args:
            message: 日志消息
            level: 日志级别，默认为'INFO'
        """
        LogManager.log(level, message) 
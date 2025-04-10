import time
import re
from typing import Optional
from .logger import Logger

class DataConverter:
    """数据转换工具类"""
    
    def __init__(self):
        self.logger = Logger()
    
    def convert_view_count(self, view_count_str: str) -> int:
        """
        将观看次数字符串转换为整数
        Args:
            view_count_str: 观看次数字符串，如"102,717次观看"或"1万次观看"或"无人观看"
        Returns:
            int: 转换后的整数
        """
        try:
            if not view_count_str:
                return 0
            
            if view_count_str == '无人观看':
                return 0
            
            number_str = view_count_str.replace('次观看', '')
            
            if '万' in number_str:
                number = float(number_str.replace('万', ''))
                return int(number * 10000)
            
            return int(number_str.replace(',', ''))
                
        except Exception as e:
            self.logger.log(f"转换观看次数时出错: {str(e)}", 'ERROR')
            return 0
    
    def convert_relative_time(self, relative_time_str: str) -> str:
        """
        将相对时间字符串转换为日期
        Args:
            relative_time_str: 相对时间字符串，如"1个月前"、"2周前"、"3天前"等
        Returns:
            str: 转换后的日期字符串，格式为'%Y-%m-%d'
        """
        try:
            current_time = time.time()
            
            if not relative_time_str:
                return time.strftime('%Y-%m-%d', time.localtime(current_time))
                
            match = re.match(r'(\d+)?(.*?)前', relative_time_str)
            if not match:
                return time.strftime('%Y-%m-%d', time.localtime(current_time))
                
            number = int(match.group(1)) if match.group(1) else 1
            unit = match.group(2)
            
            seconds = 0
            if '年' in unit:
                seconds = number * 365 * 24 * 3600
            elif '个月' in unit:
                seconds = number * 30 * 24 * 3600
            elif '周' in unit:
                seconds = number * 7 * 24 * 3600
            elif '天' in unit:
                seconds = number * 24 * 3600
            elif '小时' in unit:
                seconds = number * 3600
            elif '分钟' in unit:
                seconds = number * 60
            elif '秒' in unit:
                seconds = number
            
            target_time = current_time - seconds
            return time.strftime('%Y-%m-%d', time.localtime(target_time))
            
        except Exception as e:
            self.logger.log(f"时间转换出错: {str(e)}", 'ERROR')
            return time.strftime('%Y-%m-%d', time.localtime(current_time)) 
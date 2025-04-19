import json
import time
import os
from typing import Dict, Any

class FileHandler:
    """文件处理类"""
    
    def __init__(self):
        from .logger import Logger
        self.logger = Logger()
        # 确保responses目录存在
        self.responses_dir = "responses"
        if not os.path.exists(self.responses_dir):
            os.makedirs(self.responses_dir)
    
    def save_response_json(self, json_data: Dict[str, Any], request_count: int, is_initial: bool = False) -> None:
        """
        保存原始响应JSON到文件
        Args:
            json_data: JSON数据
            request_count: 请求计数
            is_initial: 是否是初始请求
        """
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        request_type = "initial" if is_initial else "continuation"
        filename = f"response_json_{timestamp}_{request_type}_{request_count}.json"
        filepath = os.path.join(self.responses_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            self.logger.log(f"已保存响应JSON到文件: {filepath}")
        except Exception as e:
            self.logger.log(f"保存响应JSON时出错: {str(e)}", 'ERROR') 
import base64
import brotli
from typing import Dict, Any
from .logger import Logger

class ResponseProcessor:
    """处理API响应内容的类"""
    
    def __init__(self):
        self.logger = Logger()
    
    def process_response_content(self, response: Dict[str, Any]) -> str:
        """
        处理响应内容的通用函数（处理编码和压缩）
        Args:
            response: 响应对象，包含content和headers信息
        Returns:
            str: 处理后的响应文本
        """
        try:
            response_text = response['content']['text']
            
            if response['content'].get('encoding') == 'base64':
                response_text = base64.b64decode(response_text)
                self.logger.log("已解码base64内容")
            
            if any(h['name'].lower() == 'content-encoding' and 'br' in h['value'].lower() 
                  for h in response['headers']):
                response_text = brotli.decompress(response_text)
                self.logger.log("已解压brotli内容")
            
            if isinstance(response_text, bytes):
                response_text = response_text.decode('utf-8')
                self.logger.log("已将bytes转换为字符串")
            
            return response_text
            
        except Exception as e:
            self.logger.log(f"处理响应内容时出错: {str(e)}", 'ERROR')
            raise 
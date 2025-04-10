import base64
import brotli
import json
from typing import Dict, Any, List, Optional
from .logger import Logger

class ResponseProcessor:
    """响应处理类"""
    
    def __init__(self):
        self.logger = Logger()
    
    def process_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理API响应
        Args:
            response: API响应数据
        Returns:
            处理后的数据
        """
        try:
            # 检查响应是否有效
            if not response or 'error' in response:
                self.logger.log(f"API响应无效: {response.get('error', '未知错误')}", 'ERROR')
                return {}
            
            # 提取数据
            data = response.get('data', {})
            if not data:
                self.logger.log("API响应中没有数据", 'WARNING')
                return {}
            
            return data
        except Exception as e:
            self.logger.log(f"处理API响应时出错: {str(e)}", 'ERROR')
            return {}
    
    def extract_items(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        从数据中提取项目列表
        Args:
            data: 处理后的数据
        Returns:
            项目列表
        """
        try:
            items = data.get('items', [])
            if not items:
                self.logger.log("数据中没有项目", 'WARNING')
                return []
            
            return items
        except Exception as e:
            self.logger.log(f"提取项目列表时出错: {str(e)}", 'ERROR')
            return []
    
    def get_next_page_token(self, data: Dict[str, Any]) -> Optional[str]:
        """
        获取下一页的token
        Args:
            data: 处理后的数据
        Returns:
            下一页token，如果没有则返回None
        """
        try:
            return data.get('nextPageToken')
        except Exception as e:
            self.logger.log(f"获取下一页token时出错: {str(e)}", 'ERROR')
            return None
    
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
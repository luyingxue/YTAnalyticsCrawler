from .base_model import BaseModel
from src.db import Database
import time
import random
from typing import Dict, Any, List, Optional

class ChannelBaseModel(BaseModel):
    """频道基础信息模型类，处理channel_base表的操作"""
    
    def __init__(self):
        super().__init__()
        self.db = Database()
        self.table_name = 'channel_base'
    
    def insert(self, data: Dict[str, Any]) -> bool:
        """插入单条记录"""
        try:
            result = self.db.insert(self.table_name, data)
            self.log(f"已插入频道基础数据: channel_id={data.get('channel_id')}")
            return True
        except Exception as e:
            self.log(f"插入频道基础数据失败: {str(e)}", 'ERROR')
            return False
    
    def get_by_id(self, channel_id: str) -> Optional[Dict[str, Any]]:
        """根据channel_id获取单条记录"""
        try:
            result = self.db.query(self.table_name, channel_id=channel_id)
            return result[0] if result else None
        except Exception as e:
            self.log(f"获取频道基础数据失败: {str(e)}", 'ERROR')
            return None
    
    def update(self, channel_id: str, data: Dict[str, Any]) -> bool:
        """更新记录"""
        try:
            result = self.db.client.table(self.table_name).update(data).eq('channel_id', channel_id).execute()
            self.log(f"已更新频道基础数据: channel_id={channel_id}")
            return True
        except Exception as e:
            self.log(f"更新频道基础数据失败: {str(e)}", 'ERROR')
            return False
    
    def delete(self, channel_id: str) -> bool:
        """删除记录"""
        try:
            self.db.delete(self.table_name, channel_id)
            self.log(f"已删除频道基础数据: channel_id={channel_id}")
            return True
        except Exception as e:
            self.log(f"删除频道基础数据失败: {str(e)}", 'ERROR')
            return False
    
    def get_by_condition(self, conditions: Dict[str, Any], order_by: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """根据条件查询记录"""
        try:
            result = self.db.query(self.table_name, **conditions)
            return result
        except Exception as e:
            self.log(f"查询频道基础数据失败: {str(e)}", 'ERROR')
            return []
    
    def call_rpc(self, procedure_name: str, params: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """调用存储过程
        
        Args:
            procedure_name: 存储过程名称
            params: 存储过程参数，可选
            
        Returns:
            存储过程返回的结果，如果失败则返回None
        """
        try:
            rpc_call = self.db.client.rpc(procedure_name)
            if params:
                rpc_call = rpc_call.params(params)
            result = rpc_call.execute()
            return result.data[0] if result.data and len(result.data) > 0 else None
        except Exception as e:
            self.log(f"调用存储过程 {procedure_name} 失败: {str(e)}", 'ERROR')
            return None
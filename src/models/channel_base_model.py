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
            result = self.db.client.table(self.table_name).delete().eq('channel_id', channel_id).execute()
            self.log(f"已删除频道基础数据: channel_id={channel_id}")
            return True
        except Exception as e:
            self.log(f"删除频道基础数据失败: {str(e)}", 'ERROR')
            return False
    
    def get_by_condition(self, conditions: Dict[str, Any], order_by: Optional[str] = None, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """根据条件查询记录"""
        try:
            query = self.db.client.table(self.table_name).select('*')
            for key, value in conditions.items():
                query = query.eq(key, value)
            if order_by:
                query = query.order(order_by)
            if limit:
                query = query.limit(limit)
            result = query.execute()
            return result.data
        except Exception as e:
            self.log(f"查询频道基础数据失败: {str(e)}", 'ERROR')
            return []
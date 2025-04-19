from .base_model import BaseModel
from src.db import Database
import time
import random
from typing import Dict, Any, List, Optional, Set, Tuple

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
    
    def batch_insert(self, channel_ids: Set[str]) -> Tuple[bool, str]:
        """批量插入channel_ids到channel_base表
        
        Args:
            channel_ids: 要插入的channel_id集合
            
        Returns:
            Tuple[bool, str]: (是否成功, 消息)
        """
        try:
            if not channel_ids:
                return True, "没有需要处理的频道ID"
            
            # 将集合转换为插入格式
            data = [
                {
                    'channel_id': channel_id,
                    'is_blacklist': False,
                    'is_benchmark': False
                } 
                for channel_id in channel_ids
            ]
            
            # 使用upsert批量插入
            result = self.db.client.table(self.table_name)\
                .upsert(
                    data,
                    on_conflict='channel_id'  # 指定冲突解决字段
                )\
                .execute()
            
            inserted_count = len(result.data)
            total_count = len(channel_ids)
            
            message = f"成功处理 {total_count} 个频道ID，新增 {inserted_count} 条记录"
            self.log(message)
            return True, message
            
        except Exception as e:
            error_message = f"批量插入频道数据失败: {str(e)}"
            self.log(error_message, 'ERROR')
            return False, error_message
    
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
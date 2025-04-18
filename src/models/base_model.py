import os
from ..db import Database
from typing import Dict, Any, Optional

class BaseModel:
    """基础模型类，提供数据库连接和通用方法"""
    
    def __init__(self):
        """初始化基础模型"""
        self.db = Database()
        
        from ..utils import Logger
        self.logger = Logger()
        
    def log(self, message, level='INFO'):
        """输出日志"""
        level_int = self.logger._get_level_int(level)
        self.logger.log(message, level_int)
        
    def execute_query(self, query, params=None, fetch=True):
        """执行SQL查询"""
        return self.db.execute_query(query, params, fetch)
        
    def execute_many(self, query, params_list):
        """批量执行SQL语句"""
        return self.db.execute_many(query, params_list)
        
    def transaction(self):
        """获取事务上下文管理器"""
        return self.db_client.transaction()
        
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
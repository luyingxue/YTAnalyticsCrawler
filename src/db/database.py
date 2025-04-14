from supabase import create_client
import configparser
import os
from typing import Dict, Any, List, Optional

class Database:
    _instance = None
    _client = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        config = configparser.ConfigParser()
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config.ini')
        config.read(config_path)

        supabase_url = config['supabase']['url']
        supabase_key = config['supabase']['key']
        
        self._client = create_client(supabase_url, supabase_key)

    def insert(self, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """插入单条数据"""
        result = self._client.table(table).insert(data).execute()
        return result.data[0]

    def insert_many(self, table: str, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """批量插入数据"""
        result = self._client.table(table).insert(data).execute()
        return result.data

    def update(self, table: str, id: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """更新单条数据"""
        result = self._client.table(table).update(data).eq('id', id).execute()
        return result.data[0]

    def delete(self, table: str, id: str) -> None:
        """删除单条数据"""
        self._client.table(table).delete().eq('id', id).execute()

    def get_by_id(self, table: str, id: str) -> Optional[Dict[str, Any]]:
        """根据ID查询单条数据"""
        result = self._client.table(table).select('*').eq('id', id).execute()
        return result.data[0] if result.data else None

    def get_all(self, table: str) -> List[Dict[str, Any]]:
        """查询所有数据"""
        result = self._client.table(table).select('*').execute()
        return result.data

    def query(self, table: str, **conditions) -> List[Dict[str, Any]]:
        """通用查询方法"""
        query = self._client.table(table).select('*')
        for field, value in conditions.items():
            query = query.eq(field, value)
        result = query.execute()
        return result.data

    @property
    def client(self):
        return self._client

    def get_table(self, table_name):
        return self._client.table(table_name) 
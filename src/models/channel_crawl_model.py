from .base_model import BaseModel

class ChannelCrawlModel(BaseModel):
    """频道爬取信息模型类，处理channel_crawl表的操作"""
    
    def insert(self, data):
        """插入单条记录"""
        try:
            query = """
                INSERT INTO channel_crawl (
                    channel_id, channel_name, description,
                    subscriber_count, video_count, view_count,
                    joined_date, country, crawl_date, canonical_base_url, avatar_url
                ) VALUES (
                    %(channel_id)s, %(channel_name)s, %(description)s,
                    %(subscriber_count)s, %(video_count)s, %(view_count)s,
                    %(joined_date)s, %(country)s, CURRENT_DATE, %(canonical_base_url)s, %(avatar_url)s
                )
            """
            
            self.execute_query(query, data, fetch=False)
            self.log(f"已插入频道爬取数据: channel_id={data.get('channel_id')}")
            return True
            
        except Exception as e:
            self.log(f"插入频道爬取数据失败: {str(e)}", 'ERROR')
            return False
            
    def get_by_id(self, channel_id):
        """根据channel_id获取单条记录"""
        try:
            query = """
                SELECT 
                    channel_id, channel_name, description,
                    subscriber_count, video_count, view_count,
                    joined_date, country, crawl_date, canonical_base_url, avatar_url,
                    avg_view_count, avg_subscriber_increase, daily_view_increase
                FROM channel_crawl
                WHERE channel_id = %s
            """
            
            result = self.execute_query(query, (channel_id,))
            return result[0] if result else None
            
        except Exception as e:
            self.log(f"获取频道爬取数据失败: {str(e)}", 'ERROR')
            return None
            
    def update(self, channel_id, data):
        """更新记录"""
        try:
            # 构建更新字段
            update_fields = []
            params = []
            for key, value in data.items():
                update_fields.append(f"{key} = %s")
                params.append(value)
            params.append(channel_id)
            
            query = f"""
                UPDATE channel_crawl 
                SET {', '.join(update_fields)}
                WHERE channel_id = %s
            """
            
            self.execute_query(query, tuple(params), fetch=False)
            self.log(f"已更新频道爬取数据: channel_id={channel_id}")
            return True
            
        except Exception as e:
            self.log(f"更新频道爬取数据失败: {str(e)}", 'ERROR')
            return False
            
    def delete(self, channel_id):
        """删除记录"""
        try:
            query = "DELETE FROM channel_crawl WHERE channel_id = %s"
            self.execute_query(query, (channel_id,), fetch=False)
            self.log(f"已删除频道爬取数据: channel_id={channel_id}")
            return True
            
        except Exception as e:
            self.log(f"删除频道爬取数据失败: {str(e)}", 'ERROR')
            return False
            
    def get_by_condition(self, conditions, order_by=None, limit=None):
        """根据条件查询记录"""
        try:
            query = """
                SELECT 
                    channel_id, channel_name, description,
                    subscriber_count, video_count, view_count,
                    joined_date, country, crawl_date, canonical_base_url, avatar_url,
                    avg_view_count, avg_subscriber_increase, daily_view_increase
                FROM channel_crawl
                WHERE 1=1
            """
            params = []
            
            # 添加查询条件
            for key, value in conditions.items():
                if value is not None:
                    query += f" AND {key} = %s"
                    params.append(value)
                    
            # 添加排序
            if order_by:
                query += f" ORDER BY {order_by}"
                
            # 添加限制
            if limit:
                query += f" LIMIT {limit}"
                
            result = self.execute_query(query, tuple(params))
            return result
            
        except Exception as e:
            self.log(f"查询频道爬取数据失败: {str(e)}", 'ERROR')
            return []
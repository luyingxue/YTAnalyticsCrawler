from .base_model import BaseModel

class ChannelCrawlModel(BaseModel):
    """频道爬取信息模型类，处理channel_crawl表的操作"""
    
    def insert_channel_crawl(self, channel_info):
        """插入频道爬取数据"""
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
            
            # 确保channel_info中的字段名与数据库表字段名匹配
            data = {
                'channel_id': channel_info.get('channel_id'),
                'channel_name': channel_info.get('channel_name'),
                'description': channel_info.get('description'),
                'subscriber_count': channel_info.get('subscriber_count'),
                'video_count': channel_info.get('video_count'),
                'view_count': channel_info.get('view_count'),
                'joined_date': channel_info.get('joined_date'),
                'country': channel_info.get('country'),
                'canonical_base_url': channel_info.get('canonical_url'),  # 从canonical_url映射到canonical_base_url
                'avatar_url': channel_info.get('avatar_url')
            }
            
            self.execute_query(query, data, fetch=False)
            self.log(f"已插入频道爬取数据: channel_id={data['channel_id']}")
            return True
            
        except Exception as e:
            self.log(f"插入频道爬取数据失败: {str(e)}", 'ERROR')
            return False
            
    def get_channel_history(self, channel_id, start_date=None, end_date=None):
        """获取频道历史爬取数据"""
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
            params = [channel_id]
            
            if start_date:
                query += " AND crawl_date >= %s"
                params.append(start_date)
                
            if end_date:
                query += " AND crawl_date <= %s"
                params.append(end_date)
                
            query += " ORDER BY crawl_date DESC"
            
            result = self.execute_query(query, tuple(params))
            self.log(f"已获取频道历史数据: channel_id={channel_id}, 记录数={len(result) if result else 0}")
            return result
            
        except Exception as e:
            self.log(f"获取频道历史数据失败: {str(e)}", 'ERROR')
            return []
            
    def get_channel_statistics(self, channel_id):
        """获取频道统计数据"""
        try:
            query = """
                SELECT 
                    channel_id,
                    MAX(subscriber_count) as max_subscriber_count,
                    MAX(video_count) as max_video_count,
                    MAX(view_count) as max_view_count,
                    AVG(avg_view_count) as avg_view_count,
                    AVG(avg_subscriber_increase) as avg_subscriber_increase,
                    MAX(daily_view_increase) as max_daily_view_increase
                FROM channel_crawl
                WHERE channel_id = %s
                GROUP BY channel_id
            """
            
            result = self.execute_query(query, (channel_id,))
            self.log(f"已获取频道统计数据: channel_id={channel_id}")
            return result[0] if result else None
            
        except Exception as e:
            self.log(f"获取频道统计数据失败: {str(e)}", 'ERROR')
            return None 
from .base_model import BaseModel
import time
import random

class ChannelModel(BaseModel):
    """频道模型类，处理频道相关的数据库操作"""
    
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
            
    def get_uncrawled_channel(self):
        """获取今天未爬取的频道，使用串行事务，带重试机制"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                with self.transaction() as connection:
                    cursor = connection.cursor(dictionary=True)
                    
                    try:
                        # 开始事务
                        cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE")
                        cursor.execute("START TRANSACTION")
                        
                        # 获取一条未爬取的频道
                        query = """
                            SELECT channel_id, is_benchmark, last_crawl_date
                            FROM channel_base
                            WHERE 
                                (last_crawl_date IS NULL OR last_crawl_date != CURRENT_DATE)
                                AND is_blacklist = 0
                            ORDER BY 
                                is_benchmark DESC,
                                CASE 
                                    WHEN last_crawl_date IS NULL THEN 1 
                                    ELSE 0 
                                END DESC,
                                last_crawl_date ASC
                            LIMIT 1
                            FOR UPDATE
                        """
                        
                        cursor.execute(query)
                        result = cursor.fetchone()
                        
                        if result:
                            # 立即更新last_crawl_date
                            update_query = """
                                UPDATE channel_base 
                                SET last_crawl_date = CURRENT_DATE
                                WHERE channel_id = %s
                            """
                            cursor.execute(update_query, (result['channel_id'],))
                            
                            # 构建URL
                            result['url'] = f"https://www.youtube.com/channel/{result['channel_id']}/shorts"
                            self.log(f"获取到未爬取频道: channel_id={result['channel_id']}, is_benchmark={result['is_benchmark']}, last_crawl={result['last_crawl_date']}")
                            
                            return result
                        else:
                            self.log("没有找到未爬取的频道")
                            return None
                            
                    except Exception as e:
                        raise
                        
            except Exception as e:
                if "Deadlock found" in str(e):
                    retry_count += 1
                    self.log(f"发生死锁，正在重试 ({retry_count}/{max_retries})")
                    time.sleep(random.uniform(0.1, 0.5))  # 随机延迟，避免同时重试
                    continue
                self.log(f"获取未爬取频道时出错: {str(e)}", 'ERROR')
                raise
                
        self.log(f"达到最大重试次数 ({max_retries})，放弃获取")
        return None 
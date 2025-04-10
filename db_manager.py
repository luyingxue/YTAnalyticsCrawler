from log_manager import LogManager
from db import create_connection_pool, DBError
import time
import random

class DBManager:
    """数据库管理类"""
    
    def __init__(self):
        """初始化数据库管理器"""
        self.connection_pool = create_connection_pool()
        self.logger = LogManager().get_logger('DBManager')
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
        
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
            
            self.connection_pool.execute_query(query, data)
            self.log(f"已插入频道爬取数据: channel_id={data['channel_id']}")
            return True
            
        except DBError as e:
            self.log(f"插入频道爬取数据失败: {str(e)}", 'ERROR')
            return False
            
    def get_uncrawled_channel(self):
        """获取今天未爬取的频道，使用串行事务，带重试机制"""
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                with self.connection_pool.transaction() as connection:
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
            
    def save_video_data(self, video_data):
        """保存视频数据到数据库"""
        try:
            query = """
                INSERT INTO videos (
                    video_id, title, view_count, published_date,
                    crawl_date, channel_id, channel_name, canonical_base_url
                ) VALUES (
                    %(video_id)s, %(title)s, %(view_count)s, %(published_date)s,
                    CURRENT_DATE, %(channel_id)s, %(channel_name)s, %(canonical_base_url)s
                )
            """
            
            self.connection_pool.execute_query(query, video_data)
            return True
            
        except DBError as e:
            self.log(f"保存视频数据时出错: {str(e)}", 'ERROR')
            return False
            
    def save_videos_batch(self, videos_data):
        """批量保存视频数据"""
        try:
            query = """
                INSERT INTO videos (
                    video_id, title, view_count, published_date,
                    crawl_date, channel_id, channel_name, canonical_base_url
                ) VALUES (
                    %(video_id)s, %(title)s, %(view_count)s, %(published_date)s,
                    CURRENT_DATE, %(channel_id)s, %(channel_name)s, %(canonical_base_url)s
                )
            """
            
            self.connection_pool.execute_many(query, videos_data)
            return True
            
        except DBError as e:
            self.log(f"批量保存视频数据时出错: {str(e)}", 'ERROR')
            return False
            
    def get_uncrawled_keywords(self):
        """获取未爬取的关键词"""
        try:
            query = """
                SELECT key_words
                FROM key_words
                WHERE last_crawl_date IS NULL
                OR last_crawl_date < CURRENT_DATE
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """
            
            result = self.connection_pool.execute_query(query)
            if result:
                # 更新last_crawl_date
                update_query = """
                    UPDATE key_words 
                    SET last_crawl_date = CURRENT_DATE
                    WHERE key_words = %s
                """
                self.connection_pool.execute_query(update_query, (result[0]['key_words'],))
                return result[0]['key_words']
            return None
            
        except DBError as e:
            self.log(f"获取未爬取关键词时出错: {str(e)}", 'ERROR')
            return None
            
    def save_keyword_data(self, keyword_data):
        """保存关键词数据"""
        try:
            query = """
                INSERT INTO key_words (key_words, last_crawl_date)
                VALUES (%(key_words)s, CURRENT_DATE)
            """
            
            self.connection_pool.execute_query(query, keyword_data)
            return True
            
        except DBError as e:
            self.log(f"保存关键词数据时出错: {str(e)}", 'ERROR')
            return False
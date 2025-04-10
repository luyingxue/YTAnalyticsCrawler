from log_manager import LogManager
from db import create_db_connection, create_connection_pool, DBError

class DBManager:
    """数据库管理类"""
    
    def __init__(self):
        """初始化数据库管理器"""
        self.db_connection = create_db_connection()
        self.connection_pool = create_connection_pool(pool_size=5)
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
        """获取未爬取的频道"""
        try:
            return self.connection_pool.get_uncrawled_channel()
        except DBError as e:
            self.log(f"获取未爬取频道时出错: {str(e)}", 'ERROR')
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
import mysql.connector
from mysql.connector import Error
import time
from datetime import datetime
import logging
import configparser

class DBManager:
    """数据库管理类，处理与MySQL的所有交互"""
    
    def __init__(self):
        """初始化数据库连接"""
        # 读取配置文件
        config = configparser.ConfigParser()
        config.read('config.ini')
        
        self.connection_config = {
            'host': config['database']['host'],
            'database': config['database']['database'],
            'user': config['database']['user'],
            'password': config['database']['password']
        }
        
        self.connection = None
        self.setup_logging()
        
    def setup_logging(self):
        """设置日志"""
        logging.basicConfig(
            filename='db_operations.log',
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        
    def connect(self):
        """建立数据库连接"""
        try:
            if self.connection is None or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**self.connection_config)
                logging.info("数据库连接成功")
        except Error as e:
            logging.error(f"数据库连接错误: {e}")
            raise
            
    def disconnect(self):
        """关闭数据库连接"""
        if self.connection and self.connection.is_connected():
            self.connection.close()
            logging.info("数据库连接已关闭")
            
    def save_videos(self, videos):
        """
        保存视频数据到数据库
        Args:
            videos: 视频数据列表或单个视频数据字典
        """
        if not isinstance(videos, list):
            videos = [videos]
            
        try:
            self.connect()
            cursor = self.connection.cursor()
            
            # 准备SQL语句
            insert_query = """
                INSERT INTO videos (
                    video_id, title, view_count, published_date, 
                    crawl_date, channel_id, channel_name
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s
                ) ON DUPLICATE KEY UPDATE
                    title = VALUES(title),
                    view_count = VALUES(view_count),
                    crawl_date = VALUES(crawl_date)
            """
            
            # 准备数据
            for video in videos:
                data = (
                    video.get('video_id'),
                    video.get('title'),
                    video.get('view_count'),
                    video.get('published_date'),
                    video.get('crawl_date'),
                    video.get('channel_id'),
                    video.get('channel_name')
                )
                
                try:
                    cursor.execute(insert_query, data)
                    logging.info(f"保存视频数据: {video.get('video_id')}")
                except Error as e:
                    logging.error(f"保存视频数据出错: {video.get('video_id')} - {str(e)}")
                    continue
            
            # 提交事务
            self.connection.commit()
            logging.info(f"成功保存 {len(videos)} 条视频数据")
            
        except Error as e:
            logging.error(f"数据库操作错误: {str(e)}")
            if self.connection:
                self.connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            self.disconnect()
            
    def get_video_by_id(self, video_id):
        """
        根据video_id查询视频信息
        Args:
            video_id: 视频ID
        Returns:
            dict: 视频信息字典
        """
        try:
            self.connect()
            cursor = self.connection.cursor(dictionary=True)
            
            query = "SELECT * FROM videos WHERE video_id = %s"
            cursor.execute(query, (video_id,))
            result = cursor.fetchone()
            
            return result
            
        except Error as e:
            logging.error(f"查询视频信息出错: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            self.disconnect()
            
    def get_videos_by_date_range(self, start_date, end_date):
        """
        查询日期范围内的视频
        Args:
            start_date: 开始日期
            end_date: 结束日期
        Returns:
            list: 视频信息列表
        """
        try:
            self.connect()
            cursor = self.connection.cursor(dictionary=True)
            
            query = """
                SELECT * FROM videos 
                WHERE crawl_date BETWEEN %s AND %s
                ORDER BY crawl_date DESC
            """
            cursor.execute(query, (start_date, end_date))
            results = cursor.fetchall()
            
            return results
            
        except Error as e:
            logging.error(f"查询视频列表出错: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            self.disconnect()
            
    def get_daily_stats(self):
        """
        获取每日统计数据
        Returns:
            list: 统计数据列表
        """
        try:
            self.connect()
            cursor = self.connection.cursor(dictionary=True)
            
            query = """
                SELECT 
                    crawl_date,
                    COUNT(*) as total_videos,
                    AVG(view_count) as avg_views,
                    MAX(view_count) as max_views
                FROM videos
                GROUP BY crawl_date
                ORDER BY crawl_date DESC
            """
            cursor.execute(query)
            results = cursor.fetchall()
            
            return results
            
        except Error as e:
            logging.error(f"获取统计数据出错: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            self.disconnect()
            
    def test_connection(self):
        """测试数据库连接"""
        try:
            self.connect()
            cursor = self.connection.cursor()
            
            # 测试查询
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            print(f"数据库连接成功! MySQL版本: {version[0]}")
            
            # 测试videos表是否存在
            cursor.execute("SHOW TABLES LIKE 'videos'")
            if cursor.fetchone():
                # 获取表的行数
                cursor.execute("SELECT COUNT(*) FROM videos")
                count = cursor.fetchone()[0]
                print(f"videos表存在，当前有 {count} 条记录")
                
                # 获取表结构
                cursor.execute("DESCRIBE videos")
                print("\nvideos表结构:")
                for field in cursor.fetchall():
                    print(f"字段: {field[0]}, 类型: {field[1]}, 允许NULL: {field[2]}, Key: {field[3]}")
            else:
                print("videos表不存在")
                
            return True
            
        except Error as e:
            print(f"连接测试失败: {str(e)}")
            return False
            
        finally:
            if cursor:
                cursor.close()
            self.disconnect()
            
    def get_active_search_url(self):
        """获取一个活跃的搜索URL"""
        try:
            self.connect()
            cursor = self.connection.cursor(dictionary=True)
            
            # 修改SQL语句，使用COALESCE和IS NULL来处理NULL值
            query = """
                SELECT id, url, description 
                FROM search_urls 
                WHERE is_active = TRUE 
                ORDER BY COALESCE(last_crawl_time, '1970-01-01') ASC
                LIMIT 1
            """
            cursor.execute(query)
            result = cursor.fetchone()
            
            if result:
                # 更新最后抓取时间
                update_query = """
                    UPDATE search_urls 
                    SET last_crawl_time = NOW() 
                    WHERE id = %s
                """
                cursor.execute(update_query, (result['id'],))
                self.connection.commit()
                
            return result
            
        except Error as e:
            logging.error(f"获取搜索URL出错: {str(e)}")
            raise
        finally:
            if cursor:
                cursor.close()
            self.disconnect()
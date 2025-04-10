from .base_model import BaseModel

class VideoModel(BaseModel):
    """视频模型类，处理视频相关的数据库操作"""
    
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
            
            self.execute_query(query, video_data, fetch=False)
            return True
            
        except Exception as e:
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
            
            self.execute_many(query, videos_data)
            return True
            
        except Exception as e:
            self.log(f"批量保存视频数据时出错: {str(e)}", 'ERROR')
            return False 
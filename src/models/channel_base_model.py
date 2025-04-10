from .base_model import BaseModel
import time
import random

class ChannelBaseModel(BaseModel):
    """频道基础信息模型类，处理channel_base表的操作"""
    
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
        
    def update_last_crawl_date(self, channel_id):
        """更新频道的最后爬取日期"""
        try:
            query = """
                UPDATE channel_base 
                SET last_crawl_date = CURRENT_DATE
                WHERE channel_id = %s
            """
            
            self.execute_query(query, (channel_id,), fetch=False)
            self.log(f"已更新频道最后爬取日期: channel_id={channel_id}")
            return True
            
        except Exception as e:
            self.log(f"更新频道最后爬取日期失败: {str(e)}", 'ERROR')
            return False
        
    def delete_channel(self, channel_id):
        """删除频道记录"""
        try:
            # 删除channel_base表中的记录
            query = """
                DELETE FROM channel_base
                WHERE channel_id = %s
            """
            
            self.execute_query(query, (channel_id,), fetch=False)
            self.log(f"已删除频道记录: channel_id={channel_id}")
            return True
            
        except Exception as e:
            self.log(f"删除频道记录失败: {str(e)}", 'ERROR')
            return False
            
    def add_channel(self, channel_info):
        """添加新频道到基础表"""
        try:
            query = """
                INSERT INTO channel_base (
                    channel_id, is_blacklist, is_benchmark, 
                    blacklist_reason, benchmark_type
                ) VALUES (
                    %(channel_id)s, %(is_blacklist)s, %(is_benchmark)s,
                    %(blacklist_reason)s, %(benchmark_type)s
                )
            """
            
            self.execute_query(query, channel_info, fetch=False)
            self.log(f"已添加新频道: channel_id={channel_info.get('channel_id')}")
            return True
            
        except Exception as e:
            self.log(f"添加新频道失败: {str(e)}", 'ERROR')
            return False 
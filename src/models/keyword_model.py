from .base_model import BaseModel

class KeywordModel(BaseModel):
    """关键词模型类，处理关键词相关的数据库操作"""
    
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
            
            result = self.execute_query(query)
            if result:
                # 更新last_crawl_date
                update_query = """
                    UPDATE key_words 
                    SET last_crawl_date = CURRENT_DATE
                    WHERE key_words = %s
                """
                self.execute_query(update_query, (result[0]['key_words'],), fetch=False)
                return result[0]['key_words']
            return None
            
        except Exception as e:
            self.log(f"获取未爬取关键词时出错: {str(e)}", 'ERROR')
            return None
            
    def save_keyword_data(self, keyword_data):
        """保存关键词数据"""
        try:
            query = """
                INSERT INTO key_words (key_words, last_crawl_date)
                VALUES (%(key_words)s, CURRENT_DATE)
            """
            
            self.execute_query(query, keyword_data, fetch=False)
            return True
            
        except Exception as e:
            self.log(f"保存关键词数据时出错: {str(e)}", 'ERROR')
            return False 
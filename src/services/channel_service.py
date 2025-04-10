from ..models import ChannelBaseModel, ChannelCrawlModel
from datetime import datetime, timedelta
import time
import random

class ChannelService:
    """频道服务类，处理频道相关的业务逻辑"""
    
    def __init__(self):
        """初始化频道服务"""
        self.base_model = ChannelBaseModel()
        self.crawl_model = ChannelCrawlModel()
        from src.utils import Logger
        self.logger = Logger()
        
    def log(self, message, level='INFO'):
        """输出日志"""
        level_int = self.logger._get_level_int(level)
        self.logger.log(message, level_int)
        
    def insert_channel_crawl(self, channel_info):
        """插入频道爬取数据"""
        # 数据验证
        if not self._validate_channel_info(channel_info):
            return False
            
        # 数据转换
        processed_data = self._process_channel_data(channel_info)
        
        # 调用模型层方法
        return self.crawl_model.insert(processed_data)
        
    def _validate_channel_info(self, channel_info):
        """验证频道信息"""
        if not channel_info:
            self.log("频道信息为空", 'ERROR')
            return False
            
        if not channel_info.get('channel_id'):
            self.log("缺少channel_id，无法插入数据", 'ERROR')
            return False
            
        # 检查频道是否存在于基础表中
        channel = self.base_model.get_by_id(channel_info.get('channel_id'))
        if not channel:
            self.log(f"频道 {channel_info.get('channel_id')} 不存在于基础表中", 'ERROR')
            return False
            
        # 检查是否是黑名单频道
        if channel.get('is_blacklist'):
            self.log(f"频道 {channel_info.get('channel_id')} 在黑名单中，跳过处理", 'WARNING')
            return False
            
        return True
        
    def _process_channel_data(self, channel_info):
        """处理频道数据，应用业务规则"""
        # 创建数据副本，避免修改原始数据
        processed_data = channel_info.copy()
        
        # 字段映射
        if 'canonical_url' in processed_data:
            processed_data['canonical_base_url'] = processed_data.pop('canonical_url')
            
        # 数据类型转换
        for field in ['subscriber_count', 'video_count', 'view_count']:
            if field in processed_data and processed_data[field] is not None:
                try:
                    processed_data[field] = int(processed_data[field])
                except (ValueError, TypeError):
                    processed_data[field] = 0
                    
        # 日期格式转换
        if 'joined_date' in processed_data and processed_data['joined_date']:
            try:
                if isinstance(processed_data['joined_date'], str):
                    processed_data['joined_date'] = datetime.strptime(processed_data['joined_date'], '%Y-%m-%d').date()
            except ValueError:
                processed_data['joined_date'] = None
                
        return processed_data
        
    def get_uncrawled_channel_ids(self):
        """获取今天未爬取的频道ID列表（不带事务和锁定）"""
        try:
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
            """
            
            result = self.base_model.execute_query(query)
            return result[0] if result else None
            
        except Exception as e:
            self.log(f"获取未爬取频道ID列表失败: {str(e)}", 'ERROR')
            return None
        
    def get_uncrawled_channel(self):
        """获取今天未爬取的频道，使用串行事务，带重试机制
        
        Supabase 实现方案：
        1. 创建存储过程：
        CREATE OR REPLACE FUNCTION get_next_uncrawled_channel()
        RETURNS SETOF channel_base AS $$
            UPDATE channel_base
            SET last_crawl_date = CURRENT_DATE
            WHERE channel_id = (
                SELECT channel_id
                FROM channel_base
                WHERE (last_crawl_date IS NULL OR last_crawl_date != CURRENT_DATE)
                    AND is_blacklist = false
                ORDER BY 
                    is_benchmark DESC,
                    CASE WHEN last_crawl_date IS NULL THEN 1 ELSE 0 END DESC,
                    last_crawl_date ASC
                LIMIT 1
            )
            RETURNING *;
        $$ LANGUAGE sql;
        
        2. 调用方式：
        async def get_uncrawled_channel(self):
            try:
                result = await self.supabase.rpc(
                    'get_next_uncrawled_channel'
                ).execute()
                
                if result.data:
                    channel = result.data[0]
                    channel['url'] = f"https://www.youtube.com/channel/{channel['channel_id']}/shorts"
                    return channel
                return None
                
            except Exception as e:
                self.log(f"获取未爬取频道时出错: {str(e)}", 'ERROR')
                return None
        
        优势：
        - 原子操作，无需显式事务控制
        - 无需重试机制
        - 更好的并发处理
        - 性能更优
        """
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # 使用BaseModel的transaction方法获取事务上下文
                with self.base_model.transaction() as connection:
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
                            
                            # 记录日志
                            self.log(f"获取到未爬取频道: {result['channel_id']}, 是否对标: {result['is_benchmark']}")
                            
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
        # 数据验证
        if not channel_id:
            self.log("缺少channel_id，无法更新最后爬取日期", 'ERROR')
            return False
            
        # 检查频道是否存在
        channel = self.base_model.get_by_id(channel_id)
        if not channel:
            self.log(f"频道 {channel_id} 不存在", 'ERROR')
            return False
            
        try:
            # 使用事务确保数据一致性
            with self.base_model.transaction() as connection:
                cursor = connection.cursor()
                
                # 更新last_crawl_date
                query = """
                    UPDATE channel_base 
                    SET last_crawl_date = CURRENT_DATE
                    WHERE channel_id = %s
                """
                
                cursor.execute(query, (channel_id,))
                
                # 记录日志
                self.log(f"已更新频道最后爬取日期: channel_id={channel_id}")
                
                return True
                
        except Exception as e:
            self.log(f"更新频道最后爬取日期失败: {str(e)}", 'ERROR')
            return False
        
    def delete_channel(self, channel_id):
        """删除频道记录"""
        # 数据验证
        if not channel_id:
            self.log("缺少channel_id，无法删除频道", 'ERROR')
            return False
            
        # 检查频道是否存在
        channel = self.base_model.get_by_id(channel_id)
        if not channel:
            self.log(f"频道 {channel_id} 不存在", 'ERROR')
            return False
            
        # 调用模型层方法
        return self.base_model.delete(channel_id)
        
    def add_channel(self, channel_info):
        """添加新频道到基础表"""
        # 数据验证
        if not channel_info or not channel_info.get('channel_id'):
            self.log("缺少必要信息，无法添加频道", 'ERROR')
            return False
            
        # 检查频道是否已存在
        existing_channel = self.base_model.get_by_id(channel_info.get('channel_id'))
        if existing_channel:
            self.log(f"频道 {channel_info.get('channel_id')} 已存在", 'WARNING')
            return False
            
        # 设置默认值
        if 'is_blacklist' not in channel_info:
            channel_info['is_blacklist'] = 0
            
        if 'is_benchmark' not in channel_info:
            channel_info['is_benchmark'] = 0
            
        # 调用模型层方法
        return self.base_model.insert(channel_info)
        
    def get_channel_history(self, channel_id, start_date=None, end_date=None):
        """获取频道历史爬取数据"""
        # 数据验证
        if not channel_id:
            self.log("缺少channel_id，无法获取历史数据", 'ERROR')
            return []
            
        # 检查频道是否存在
        channel = self.base_model.get_by_id(channel_id)
        if not channel:
            self.log(f"频道 {channel_id} 不存在", 'ERROR')
            return []
            
        # 日期格式转换
        if start_date and isinstance(start_date, str):
            try:
                start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
            except ValueError:
                self.log(f"开始日期格式错误: {start_date}", 'ERROR')
                start_date = None
                
        if end_date and isinstance(end_date, str):
            try:
                end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
            except ValueError:
                self.log(f"结束日期格式错误: {end_date}", 'ERROR')
                end_date = None
                
        # 获取基础数据
        history_data = self.crawl_model.get_by_condition(
            {'channel_id': channel_id},
            order_by='crawl_date DESC'
        )
        
        # 按日期排序
        if history_data:
            history_data.sort(key=lambda x: x['crawl_date'], reverse=True)
            
        return history_data
        
    def get_channel_statistics(self, channel_id):
        """获取频道统计数据"""
        # 数据验证
        if not channel_id:
            self.log("缺少channel_id，无法获取统计数据", 'ERROR')
            return None
            
        # 检查频道是否存在
        channel = self.base_model.get_by_id(channel_id)
        if not channel:
            self.log(f"频道 {channel_id} 不存在", 'ERROR')
            return None
            
        # 获取原始数据
        raw_data = self.crawl_model.get_by_condition({'channel_id': channel_id})
        if not raw_data:
            return None
            
        # 计算统计数据
        stats = {
            'channel_id': channel_id,
            'max_subscriber_count': max(d['subscriber_count'] for d in raw_data if d['subscriber_count']),
            'max_video_count': max(d['video_count'] for d in raw_data if d['video_count']),
            'max_view_count': max(d['view_count'] for d in raw_data if d['view_count']),
            'avg_view_count': sum(d['avg_view_count'] for d in raw_data if d['avg_view_count']) / len(raw_data),
            'avg_subscriber_increase': sum(d['avg_subscriber_increase'] for d in raw_data if d['avg_subscriber_increase']) / len(raw_data),
            'max_daily_view_increase': max(d['daily_view_increase'] for d in raw_data if d['daily_view_increase'])
        }
        
        return stats
        
    def get_latest_crawl_data(self, channel_id):
        """获取频道最新的爬取数据"""
        # 数据验证
        if not channel_id:
            self.log("缺少channel_id，无法获取最新爬取数据", 'ERROR')
            return None
            
        # 检查频道是否存在
        channel = self.base_model.get_by_id(channel_id)
        if not channel:
            self.log(f"频道 {channel_id} 不存在", 'ERROR')
            return None
            
        # 获取原始数据
        raw_data = self.crawl_model.get_by_condition(
            {'channel_id': channel_id},
            order_by='crawl_date DESC',
            limit=1
        )
        
        return raw_data[0] if raw_data else None
        
    def add_new_channel(self, channel_info, crawl_data=None):
        """添加新频道并初始化爬取数据（跨表操作）"""
        # 添加频道到基础表
        if not self.add_channel(channel_info):
            return False
            
        # 如果有爬取数据，添加到爬取表
        if crawl_data:
            crawl_data['channel_id'] = channel_info['channel_id']
            return self.insert_channel_crawl(crawl_data)
            
        return True
        
    def get_channel_growth_rate(self, channel_id, days=30):
        """计算频道增长率（业务逻辑）"""
        # 获取频道历史数据
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days)
        
        history = self.get_channel_history(channel_id, start_date, end_date)
        if not history or len(history) < 2:
            self.log(f"频道 {channel_id} 历史数据不足，无法计算增长率", 'WARNING')
            return None
            
        # 按日期排序
        history.sort(key=lambda x: x['crawl_date'])
        
        # 计算增长率
        oldest_data = history[0]
        newest_data = history[-1]
        
        # 订阅数增长率
        if oldest_data['subscriber_count'] and oldest_data['subscriber_count'] > 0:
            subscriber_growth = (newest_data['subscriber_count'] - oldest_data['subscriber_count']) / oldest_data['subscriber_count'] * 100
        else:
            subscriber_growth = 0
            
        # 视频数增长率
        if oldest_data['video_count'] and oldest_data['video_count'] > 0:
            video_growth = (newest_data['video_count'] - oldest_data['video_count']) / oldest_data['video_count'] * 100
        else:
            video_growth = 0
            
        # 观看数增长率
        if oldest_data['view_count'] and oldest_data['view_count'] > 0:
            view_growth = (newest_data['view_count'] - oldest_data['view_count']) / oldest_data['view_count'] * 100
        else:
            view_growth = 0
            
        return {
            'channel_id': channel_id,
            'subscriber_growth_rate': round(subscriber_growth, 2),
            'video_growth_rate': round(video_growth, 2),
            'view_growth_rate': round(view_growth, 2),
            'period_days': days
        } 
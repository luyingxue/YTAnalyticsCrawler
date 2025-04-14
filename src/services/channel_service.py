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
        
        # 打印处理后的数据
        self.log(f"处理后的频道数据: {processed_data}")
        
        # 分解数据为两部分
        channel_id = processed_data.get('channel_id')
        
        # 1. 更新channel_base表
        base_data = {
            'channel_name': processed_data.get('channel_name'),
            'description': processed_data.get('description'),
            'canonical_base_url': processed_data.get('canonical_base_url'),
            'avatar_url': processed_data.get('avatar_url'),
            'joined_date': processed_data.get('joined_date'),
            'country': processed_data.get('country'),
            'last_crawl_date': datetime.now().date().isoformat(),
            'new_videos_info': processed_data.get('new_videos_info')
        }
        
        # 移除None值
        base_data = {k: v for k, v in base_data.items() if v is not None}
        
        # 更新channel_base表
        if base_data:
            update_result = self.base_model.update(channel_id, base_data)
            if not update_result:
                self.log(f"更新频道基础数据失败: channel_id={channel_id}", 'ERROR')
                return False
        
        # 2. 插入channel_crawl表
        crawl_data = {
            'channel_id': channel_id,
            'subscriber_count': processed_data.get('subscriber_count'),
            'video_count': processed_data.get('video_count'),
            'view_count': processed_data.get('view_count')
        }
        
        # 移除None值
        crawl_data = {k: v for k, v in crawl_data.items() if v is not None}
        
        # 调用模型层方法插入爬取数据
        return self.crawl_model.insert(crawl_data)
        
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
                    self.log(f"字段 {field} 转换失败: {processed_data[field]}", 'WARNING')
                    processed_data[field] = None
                    
        return processed_data
        
    def get_uncrawled_channel(self):
        """获取今天未爬取的频道"""
        try:
            # 调用存储过程获取未爬取的频道
            result = self.base_model.call_rpc('get_next_uncrawled_channel')
            
            if result:
                # 构建URL
                result['url'] = f"https://www.youtube.com/channel/{result['channel_id']}/shorts"
                
                # 记录日志
                self.log(f"获取到未爬取频道: {result['channel_id']}, 是否对标: {result.get('is_benchmark')}")
                
                return result
            else:
                self.log("没有找到未爬取的频道")
                return None
                
        except Exception as e:
            self.log(f"获取未爬取频道时出错: {str(e)}", "ERROR")
            return None
            
    def delete_channel(self, channel_id):
        """删除频道"""
        try:
            # 删除频道基础数据，crawl表数据会自动级联删除
            base_result = self.base_model.delete(channel_id)
            if not base_result:
                self.log(f"删除频道基础数据失败: {channel_id}", 'ERROR')
                return False
                
            self.log(f"成功删除频道: {channel_id}")
            return True
            
        except Exception as e:
            self.log(f"删除频道时出错: {str(e)}", 'ERROR')
            return False
            
    def add_channel(self, channel_info):
        """添加新频道"""
        try:
            # 数据验证
            if not channel_info.get('channel_id'):
                self.log("缺少channel_id，无法添加频道", 'ERROR')
                return False
                
            # 检查频道是否已存在
            existing_channel = self.base_model.get_by_id(channel_info.get('channel_id'))
            if existing_channel:
                self.log(f"频道已存在: {channel_info.get('channel_id')}", 'WARNING')
                return False
                
            # 添加频道基础数据
            base_result = self.base_model.insert(channel_info)
            if not base_result:
                self.log(f"添加频道基础数据失败: {channel_info.get('channel_id')}", 'ERROR')
                return False
                
            self.log(f"成功添加频道: {channel_info.get('channel_id')}")
            return True
            
        except Exception as e:
            self.log(f"添加频道时出错: {str(e)}", 'ERROR')
            return False
            
    def get_channel_history(self, channel_id, start_date=None, end_date=None):
        """获取频道历史数据"""
        try:
            # 如果没有指定日期范围，默认获取最近30天的数据
            if not start_date:
                start_date = datetime.now().date() - timedelta(days=30)
            if not end_date:
                end_date = datetime.now().date()
                
            # 获取频道爬取历史
            history = self.crawl_model.get_history(channel_id, start_date, end_date)
            
            if history:
                self.log(f"成功获取频道历史数据: {channel_id}")
                return history
            else:
                self.log(f"没有找到频道历史数据: {channel_id}")
                return None
                
        except Exception as e:
            self.log(f"获取频道历史数据时出错: {str(e)}", 'ERROR')
            return None
            
    def get_channel_statistics(self, channel_id):
        """获取频道统计数据"""
        try:
            # 获取频道基础信息
            base_info = self.base_model.get_by_id(channel_id)
            if not base_info:
                self.log(f"频道不存在: {channel_id}", 'ERROR')
                return None
                
            # 获取最新爬取数据
            latest_crawl = self.crawl_model.get_latest(channel_id)
            
            # 获取历史数据
            history = self.crawl_model.get_history(channel_id)
            
            # 组装统计数据
            statistics = {
                'base_info': base_info,
                'latest_data': latest_crawl,
                'history': history
            }
            
            self.log(f"成功获取频道统计数据: {channel_id}")
            return statistics
            
        except Exception as e:
            self.log(f"获取频道统计数据时出错: {str(e)}", 'ERROR')
            return None
            
    def get_latest_crawl_data(self, channel_id):
        """获取频道最新爬取数据"""
        try:
            # 获取最新爬取数据
            latest_data = self.crawl_model.get_latest(channel_id)
            
            if latest_data:
                self.log(f"成功获取频道最新爬取数据: {channel_id}")
                return latest_data
            else:
                self.log(f"没有找到频道最新爬取数据: {channel_id}")
                return None
                
        except Exception as e:
            self.log(f"获取频道最新爬取数据时出错: {str(e)}", 'ERROR')
            return None
            
    def add_new_channel(self, channel_info, crawl_data=None):
        """添加新频道并插入爬取数据"""
        try:
            # 添加频道基础数据
            base_result = self.add_channel(channel_info)
            if not base_result:
                return False
                
            # 如果有爬取数据，插入爬取数据
            if crawl_data:
                crawl_data['channel_id'] = channel_info.get('channel_id')
                crawl_result = self.insert_channel_crawl(crawl_data)
                if not crawl_result:
                    self.log(f"插入频道爬取数据失败: {channel_info.get('channel_id')}", 'ERROR')
                    return False
                    
            return True
            
        except Exception as e:
            self.log(f"添加新频道时出错: {str(e)}", 'ERROR')
            return False
            
    def get_channel_growth_rate(self, channel_id, days=30):
        """计算频道增长率"""
        try:
            # 获取指定时间范围内的历史数据
            end_date = datetime.now().date()
            start_date = end_date - timedelta(days=days)
            
            history = self.crawl_model.get_history(channel_id, start_date, end_date)
            if not history or len(history) < 2:
                self.log(f"历史数据不足，无法计算增长率: {channel_id}", 'WARNING')
                return None
                
            # 按日期排序
            history.sort(key=lambda x: x.get('crawl_date'))
            
            # 计算增长率
            first_data = history[0]
            last_data = history[-1]
            
            growth_rate = {
                'subscriber_growth': self._calculate_growth_rate(
                    first_data.get('subscriber_count'),
                    last_data.get('subscriber_count'),
                    days
                ),
                'video_growth': self._calculate_growth_rate(
                    first_data.get('video_count'),
                    last_data.get('video_count'),
                    days
                ),
                'view_growth': self._calculate_growth_rate(
                    first_data.get('view_count'),
                    last_data.get('view_count'),
                    days
                )
            }
            
            self.log(f"成功计算频道增长率: {channel_id}")
            return growth_rate
            
        except Exception as e:
            self.log(f"计算频道增长率时出错: {str(e)}", 'ERROR')
            return None
            
    def _calculate_growth_rate(self, start_value, end_value, days):
        """计算增长率"""
        if not start_value or not end_value or start_value == 0:
            return None
            
        # 计算日增长率
        daily_growth = (end_value - start_value) / start_value / days
        
        # 转换为百分比
        return daily_growth * 100 
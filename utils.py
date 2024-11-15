import base64
import brotli
import re
import json
import time
from db_manager import DBManager
from log_manager import LogManager

class Utils:
    """通用工具类，包含各种辅助方法"""
    
    logger = LogManager().get_logger('Utils')
    
    @staticmethod
    def log(message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
    
    @staticmethod
    def process_response_content(response):
        """
        处理响应内容的通用函数（处理编码和压缩）
        Args:
            response: 响应对象，包含content和headers信息
        Returns:
            str: 处理后的响应文本
        """
        try:
            response_text = response['content']['text']
            
            if response['content'].get('encoding') == 'base64':
                response_text = base64.b64decode(response_text)
                Utils.log("已解码base64内容")
            
            if any(h['name'].lower() == 'content-encoding' and 'br' in h['value'].lower() 
                  for h in response['headers']):
                response_text = brotli.decompress(response_text)
                Utils.log("已解压brotli内容")
            
            if isinstance(response_text, bytes):
                response_text = response_text.decode('utf-8')
                Utils.log("已将bytes转换为字符串")
            
            return response_text
            
        except Exception as e:
            Utils.log(f"处理响应内容时出错: {str(e)}", 'ERROR')
            raise
    
    @staticmethod
    def convert_view_count(view_count_str):
        """
        将观看次数字符串转换为整数
        Args:
            view_count_str: 观看次数字符串，如"102,717次观看"或"1万次观看"或"无人观看"
        Returns:
            int: 转换后的整数
        """
        try:
            if not view_count_str:
                return 0
            
            # 处理"无人观看"的情况
            if view_count_str == '无人观看':
                return 0
            
            number_str = view_count_str.replace('次观看', '')
            
            if '万' in number_str:
                number = float(number_str.replace('万', ''))
                return int(number * 10000)
            
            return int(number_str.replace(',', ''))
                
        except Exception as e:
            Utils.log(f"转换观看次数时出错: {str(e)}", 'ERROR')
            return 0
    
    @staticmethod
    def convert_relative_time(relative_time_str):
        """
        将相对时间字符串转换为日期
        Args:
            relative_time_str: 相对时间字符串，如"1个月前"、"2周前"、"3天前"等
        Returns:
            str: 转换后的日期字符串，格式为'%Y-%m-%d'
        """
        try:
            current_time = time.time()
            
            if not relative_time_str:
                return time.strftime('%Y-%m-%d', time.localtime(current_time))
                
            match = re.match(r'(\d+)?(.*?)前', relative_time_str)
            if not match:
                return time.strftime('%Y-%m-%d', time.localtime(current_time))
                
            number = int(match.group(1)) if match.group(1) else 1
            unit = match.group(2)
            
            seconds = 0
            if '年' in unit:
                seconds = number * 365 * 24 * 3600
            elif '个月' in unit:
                seconds = number * 30 * 24 * 3600
            elif '周' in unit:
                seconds = number * 7 * 24 * 3600
            elif '天' in unit:
                seconds = number * 24 * 3600
            elif '小时' in unit:
                seconds = number * 3600
            elif '分钟' in unit:
                seconds = number * 60
            elif '秒' in unit:
                seconds = number
            
            target_time = current_time - seconds
            return time.strftime('%Y-%m-%d', time.localtime(target_time))
            
        except Exception as e:
            Utils.log(f"时间转换出错: {str(e)}", 'ERROR')
            return time.strftime('%Y-%m-%d', time.localtime(current_time))
    
    @staticmethod
    def analyze_and_store_json_response_first(json_data):
        """分析并存储第一次JSON响应数据"""
        Utils.log("\n分析JSON响应...")
        
        effective_contents = json_data.get('onResponseReceivedCommands', [{}])[0] \
                       .get('reloadContinuationItemsCommand', {}) \
                       .get('continuationItems', [{}])[0] \
                       .get('twoColumnSearchResultsRenderer', {}) \
                       .get('primaryContents', {}) \
                       .get('sectionListRenderer', {}) \
                       .get('contents', [{}])[0] \
                       .get('itemSectionRenderer', {}) \
                       .get('contents', [])

        results = []
        Utils.log(f"\n找到 {len(effective_contents)} 个视频内容")
        
        for item in effective_contents:
            videoRenderer = item.get('videoRenderer', {})
            if not videoRenderer:
                continue
                
            video_id = videoRenderer.get('videoId', '')
            if not video_id:
                continue
                
            video_data = {
                'video_id': video_id,
                'title': videoRenderer.get('title', {}).get('runs', [{}])[0].get('text', ''),
                'view_count': Utils.convert_view_count(videoRenderer.get('viewCountText', {}).get('simpleText', '')),
                'published_date': Utils.convert_relative_time(videoRenderer.get('publishedTimeText', {}).get('simpleText', '')),
                'crawl_date': time.strftime('%Y-%m-%d'),
                'channel_id': videoRenderer.get('longBylineText', {}).get('runs', [{}])[0] \
                    .get('navigationEndpoint', {}).get('browseEndpoint', {}).get('browseId', ''),
                'channel_name': videoRenderer.get('longBylineText', {}).get('runs', [{}])[0].get('text', ''),
                'canonical_base_url': videoRenderer.get('longBylineText', {}).get('runs', [{}])[0] \
                    .get('navigationEndpoint', {}).get('browseEndpoint', {}).get('canonicalBaseUrl', '')
            }
            
            results.append(video_data)
        
        if results:
            db = DBManager()
            db.batch_insert_videos(results)
        
        Utils.log(f"\n总共解析了 {len(results)} 个视频的数据")
        return results
    
    @staticmethod
    def analyze_and_store_json_response_else(json_data):
        """分析并存储后续JSON响应数据"""
        Utils.log("\n分析JSON响应...")
        
        try:
            contents = json_data['onResponseReceivedCommands'][0] \
                ['appendContinuationItemsAction']['continuationItems'][0] \
                ['itemSectionRenderer']['contents']
            Utils.log(f"\n找到 {len(contents)} 个视频内容")
        except (KeyError, IndexError):
            Utils.log("未找到视频内容")
            return []

        results = []
        for item in contents:
            try:
                renderer = item['videoRenderer']
                video_id = renderer['videoId']
                
                video_data = {
                    'video_id': video_id,
                    'title': renderer['title']['runs'][0]['text'],
                    'view_count': Utils.convert_view_count(renderer['viewCountText']['simpleText']),
                    'published_date': Utils.convert_relative_time(renderer['publishedTimeText']['simpleText']),
                    'crawl_date': time.strftime('%Y-%m-%d'),
                    'channel_id': renderer['longBylineText']['runs'][0]['navigationEndpoint']['browseEndpoint']['browseId'],
                    'channel_name': renderer['longBylineText']['runs'][0]['text'],
                    'canonical_base_url': renderer['longBylineText']['runs'][0]['navigationEndpoint']['browseEndpoint']['canonicalBaseUrl']
                }
                
                results.append(video_data)
                
            except Exception as e:
                Utils.log(f"处理视频时出错: {str(e)}")
                continue
        
        if results:
            db = DBManager()
            db.batch_insert_videos(results)
        
        Utils.log(f"\n总共解析了 {len(results)} 个视频的数据")
        return results
    
    @staticmethod
    def save_response_json(json_data, request_count, is_initial=False):
        """
        保存原始响应JSON到文件
        Args:
            json_data: JSON数据
            request_count: 请求计数
            is_initial: 是否是初始请求
        """
        # 使用时间戳创建文件名
        timestamp = time.strftime('%Y%m%d_%H%M%S')
        request_type = "initial" if is_initial else "continuation"
        filename = f"response_json_{timestamp}_{request_type}_{request_count}.json"
        
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            Utils.log(f"已保存响应JSON到文件: {filename}")
        except Exception as e:
            Utils.log(f"保存响应JSON时出错: {str(e)}", 'ERROR')
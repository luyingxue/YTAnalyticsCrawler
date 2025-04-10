import base64
import brotli
import re
import json
import time
from src.services import VideoService
from log_manager import LogManager
import traceback

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
            # 使用VideoService替代DBManager
            video_service = VideoService()
            video_service.save_videos_batch(results)
        
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
            # 使用VideoService替代DBManager
            video_service = VideoService()
            video_service.save_videos_batch(results)
        
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
    
    @staticmethod
    def analyze_channel_json_response(json_data, page_channel_name=None):
        """分析频道JSON响应数据"""
        try:
            # 获取about信息
            about_renderer = None
            if 'onResponseReceivedEndpoints' in json_data:
                endpoints = json_data.get('onResponseReceivedEndpoints', [])
                for endpoint in endpoints:
                    if 'appendContinuationItemsAction' in endpoint:
                        items = endpoint.get('appendContinuationItemsAction', {}).get('continuationItems', [])
                        for item in items:
                            if 'aboutChannelRenderer' in item:
                                about_renderer = item.get('aboutChannelRenderer', {}).get('metadata', {}).get('aboutChannelViewModel', {})
                                Utils.log(f"找到aboutChannelViewModel: {json.dumps(about_renderer, ensure_ascii=False)[:1000]}")
                                break
            
            if not about_renderer:
                Utils.log("未找到aboutChannelViewModel，尝试其他路径")
                # 尝试其他可能的路径
                if 'metadata' in json_data:
                    about_renderer = json_data.get('metadata', {}).get('channelMetadataRenderer', {})
                    Utils.log(f"从metadata路径找到信息: {json.dumps(about_renderer, ensure_ascii=False)[:1000]}")
            
            if not about_renderer:
                Utils.log("无法找到频道信息")
                return None
            
            # 从about中提取信息
            channel_data = {}
            
            # 频道ID - 必需字段
            channel_id = about_renderer.get('channelId', '')
            if not channel_id:
                Utils.log("未找到channel_id，放弃处理")
                return None
            channel_data['channel_id'] = channel_id
            Utils.log(f"提取到channel_id: {channel_id}")
            
            # 频道名称
            channel_name = ''
            canonical_url = about_renderer.get('canonicalChannelUrl', '')
            if '@' in canonical_url:
                channel_name = canonical_url.split('@')[1].split('/')[0]
            if not channel_name:
                channel_name = about_renderer.get('title', {}).get('simpleText', '') or \
                             about_renderer.get('title', '')
            
            # 使用页面获取的channel_name，如果没有则使用原有逻辑
            if page_channel_name != None:
                channel_name = page_channel_name
            channel_data['channel_name'] = channel_name
            Utils.log(f"提取到channel_name: {channel_name}")
            
            # 描述
            description = about_renderer.get('description', '') or \
                         about_renderer.get('description', {}).get('simpleText', '')
            channel_data['description'] = description[:1000] if description else ''
            Utils.log(f"提取到description: {description}")
            
            # 国家
            country = about_renderer.get('country', '') or \
                     about_renderer.get('country', {}).get('simpleText', '')
            channel_data['country'] = country[:50] if country else ''
            Utils.log(f"提取到country: {country}")
            
            # 订阅者数量
            subscriber_text = about_renderer.get('subscriberCountText', '') or \
                            about_renderer.get('subscriberCount', '')
            subscriber_count = 0
            try:
                if isinstance(subscriber_text, dict):
                    subscriber_text = subscriber_text.get('simpleText', '0')
                subscriber_str = str(subscriber_text)
                
                # 处理英文格式: "K subscribers" / "M subscribers"
                if 'subscribers' in subscriber_str:
                    num_str = subscriber_str.replace(' subscribers', '').replace(',', '')
                    if 'K' in num_str:
                        subscriber_count = int(float(num_str.replace('K', '')) * 1000)
                    elif 'M' in num_str:
                        subscriber_count = int(float(num_str.replace('M', '')) * 1000000)
                    else:
                        subscriber_count = int(num_str)
                else:
                    subscriber_count = int(subscriber_str.replace(',', ''))
            except (ValueError, TypeError, AttributeError) as e:
                Utils.log(f"转换订阅者数量出错: {str(e)}, 原始文本: {subscriber_text}")
            channel_data['subscriber_count'] = subscriber_count
            Utils.log(f"提取到subscriber_count: {subscriber_count}")
            
            # 观看次数
            view_text = about_renderer.get('viewCountText', '') or \
                       about_renderer.get('viewCount', '')
            view_count = 0
            try:
                if isinstance(view_text, dict):
                    view_text = view_text.get('simpleText', '0')
                view_str = str(view_text)
                
                # 处理英文格式: "views"
                if 'views' in view_str:
                    view_count = int(view_str.replace(' views', '').replace(',', ''))
                else:
                    view_count = int(view_str.replace(',', ''))
            except (ValueError, TypeError, AttributeError) as e:
                Utils.log(f"转换观看次数出错: {str(e)}, 原始文本: {view_text}")
            channel_data['view_count'] = view_count
            Utils.log(f"提取到view_count: {view_count}")
            
            # 加入日期
            joined_text = None
            if isinstance(about_renderer.get('joinedDateText', {}), dict):
                joined_text = about_renderer.get('joinedDateText', {}).get('content', '')
            else:
                joined_text = about_renderer.get('joinedDateText', '')
            
            joined_date = None
            if joined_text:
                try:
                    joined_str = str(joined_text)
                    # 处理英文格式: "Joined Oct 5, 2024"
                    if 'Joined' in joined_str:
                        month_dict = {
                            'Jan': '01', 'Feb': '02', 'Mar': '03', 'Apr': '04',
                            'May': '05', 'Jun': '06', 'Jul': '07', 'Aug': '08',
                            'Sep': '09', 'Oct': '10', 'Nov': '11', 'Dec': '12'
                        }
                        parts = joined_str.replace('Joined ', '').replace(',', '').split()
                        if len(parts) >= 3:
                            month = month_dict.get(parts[0], '01')
                            day = parts[1].zfill(2)
                            year = parts[2]
                            joined_date = f"{year}-{month}-{day}"
                except (IndexError, ValueError, AttributeError) as e:
                    Utils.log(f"转换加入日期出错: {str(e)}, 原始文本: {joined_text}")
            channel_data['joined_date'] = joined_date
            Utils.log(f"提取到joined_date: {joined_date}")
            
            # 视频数量
            video_text = about_renderer.get('videoCountText', '') or \
                        about_renderer.get('videoCount', '')
            video_count = 0
            try:
                if isinstance(video_text, dict):
                    video_text = video_text.get('simpleText', '0')
                video_str = str(video_text)
                
                # 处理英文格式: " videos"
                if ' videos' in video_str:
                    video_count = int(video_str.replace(' videos', '').replace(',', ''))
                else:
                    video_count = int(video_str.replace(',', ''))
            except (ValueError, TypeError, AttributeError) as e:
                Utils.log(f"转换视频数量出错: {str(e)}, 原始文本: {video_text}")
            channel_data['video_count'] = video_count
            Utils.log(f"提取到video_count: {video_count}")
            
            # 频道URL
            canonical_url = about_renderer.get('canonicalChannelUrl', '') or \
                           about_renderer.get('canonicalBaseUrl', '')
            if canonical_url.startswith('http://www.youtube.com') or canonical_url.startswith('https://www.youtube.com'):
                canonical_url = canonical_url.split('youtube.com')[1]
            channel_data['canonical_url'] = canonical_url
            Utils.log(f"提取到canonical_url: {canonical_url}")
            
            # 验证必要字段
            if not channel_data['channel_id'] or not channel_data['channel_name']:
                Utils.log("缺少必要字段，放弃处理")
                return None
            
            # 打印最终结果用于调试
            Utils.log(f"解析结果: {json.dumps(channel_data, ensure_ascii=False)}")
            
            return channel_data
            
        except Exception as e:
            Utils.log(f"分析频道JSON数据时出错: {str(e)}", 'ERROR')
            Utils.log(f"错误详情: {traceback.format_exc()}", 'ERROR')
            return None
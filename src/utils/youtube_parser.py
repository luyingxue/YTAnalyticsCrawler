import json
import time
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from .data_converter import DataConverter
from src.services import VideoService
from .logger import Logger

@dataclass
class VideoData:
    """视频数据结构"""
    video_id: str
    title: str
    view_count: int
    published_date: str
    crawl_date: str
    channel_id: str
    channel_name: str
    canonical_base_url: str

class YouTubeParser:
    """YouTube数据解析类"""
    
    def __init__(self):
        self.logger = Logger()
        self.data_converter = DataConverter()
        self.video_service = VideoService()
    
    def analyze_and_store_json_response_first(self, json_data: Dict[str, Any]) -> List[VideoData]:
        """分析并存储第一次JSON响应数据"""
        self.logger.log("\n分析JSON响应...")
        
        try:
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
            self.logger.log(f"\n找到 {len(effective_contents)} 个视频内容")
            
            for item in effective_contents:
                video_data = self._extract_video_data(item)
                if video_data:
                    results.append(video_data)
            
            if results:
                self.video_service.save_videos_batch(results)
            
            self.logger.log(f"\n总共解析了 {len(results)} 个视频的数据")
            return results
            
        except Exception as e:
            self.logger.log(f"分析首次JSON响应时出错: {str(e)}", 'ERROR')
            return []
    
    def analyze_and_store_json_response_else(self, json_data: Dict[str, Any]) -> List[VideoData]:
        """分析并存储后续JSON响应数据"""
        self.logger.log("\n分析JSON响应...")
        
        try:
            contents = json_data['onResponseReceivedCommands'][0] \
                ['appendContinuationItemsAction']['continuationItems'][0] \
                ['itemSectionRenderer']['contents']
            self.logger.log(f"\n找到 {len(contents)} 个视频内容")
            
            results = []
            for item in contents:
                video_data = self._extract_video_data(item)
                if video_data:
                    results.append(video_data)
            
            if results:
                self.video_service.save_videos_batch(results)
            
            self.logger.log(f"\n总共解析了 {len(results)} 个视频的数据")
            return results
            
        except Exception as e:
            self.logger.log(f"分析后续JSON响应时出错: {str(e)}", 'ERROR')
            return []
    
    def analyze_channel_json_response(self, json_data: Dict[str, Any], page_channel_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """分析频道JSON响应数据"""
        self.logger.log("\n分析频道JSON响应数据...")
        
        try:
            about_renderer = self._get_about_renderer(json_data)
            if not about_renderer:
                self.logger.log("无法找到频道信息")
                return None
            
            channel_data = self._extract_channel_data(about_renderer, page_channel_name)
            if not channel_data:
                return None
            
            self.logger.log(f"解析结果: {json.dumps(channel_data, ensure_ascii=False)}")
            return channel_data
            
        except Exception as e:
            self.logger.log(f"分析频道JSON数据时出错: {str(e)}", 'ERROR')
            return None
    
    def _extract_video_data(self, item: Dict[str, Any]) -> Optional[VideoData]:
        """从视频项中提取数据"""
        try:
            video_renderer = item.get('videoRenderer', {})
            if not video_renderer:
                return None
                
            video_id = video_renderer.get('videoId', '')
            if not video_id:
                return None
                
            return VideoData(
                video_id=video_id,
                title=video_renderer.get('title', {}).get('runs', [{}])[0].get('text', ''),
                view_count=self.data_converter.convert_view_count(
                    video_renderer.get('viewCountText', {}).get('simpleText', '')
                ),
                published_date=self.data_converter.convert_relative_time(
                    video_renderer.get('publishedTimeText', {}).get('simpleText', '')
                ),
                crawl_date=time.strftime('%Y-%m-%d'),
                channel_id=video_renderer.get('longBylineText', {}).get('runs', [{}])[0]
                    .get('navigationEndpoint', {}).get('browseEndpoint', {}).get('browseId', ''),
                channel_name=video_renderer.get('longBylineText', {}).get('runs', [{}])[0].get('text', ''),
                canonical_base_url=video_renderer.get('longBylineText', {}).get('runs', [{}])[0]
                    .get('navigationEndpoint', {}).get('browseEndpoint', {}).get('canonicalBaseUrl', '')
            )
            
        except Exception as e:
            self.logger.log(f"提取视频数据时出错: {str(e)}", 'ERROR')
            return None
    
    def _get_about_renderer(self, json_data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """获取频道about信息"""
        if 'onResponseReceivedEndpoints' in json_data:
            endpoints = json_data.get('onResponseReceivedEndpoints', [])
            for endpoint in endpoints:
                if 'appendContinuationItemsAction' in endpoint:
                    items = endpoint.get('appendContinuationItemsAction', {}).get('continuationItems', [])
                    for item in items:
                        if 'aboutChannelRenderer' in item:
                            about_renderer = item.get('aboutChannelRenderer', {}).get('metadata', {}).get('aboutChannelViewModel', {})
                            self.logger.log(f"找到aboutChannelViewModel: {json.dumps(about_renderer, ensure_ascii=False)[:1000]}")
                            return about_renderer
        
        if 'metadata' in json_data:
            about_renderer = json_data.get('metadata', {}).get('channelMetadataRenderer', {})
            self.logger.log(f"从metadata路径找到信息: {json.dumps(about_renderer, ensure_ascii=False)[:1000]}")
            return about_renderer
        
        return None
    
    def _extract_channel_data(self, about_renderer: Dict[str, Any], page_channel_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """提取频道数据"""
        try:
            channel_data = {}
            
            # 频道ID
            channel_id = about_renderer.get('channelId', '')
            if not channel_id:
                self.logger.log("未找到channel_id，放弃处理")
                return None
            channel_data['channel_id'] = channel_id
            
            # 频道名称
            channel_name = ''
            canonical_url = about_renderer.get('canonicalChannelUrl', '')
            if '@' in canonical_url:
                channel_name = canonical_url.split('@')[1].split('/')[0]
            if not channel_name:
                channel_name = about_renderer.get('title', {}).get('simpleText', '') or \
                             about_renderer.get('title', '')
            
            if page_channel_name is not None:
                channel_name = page_channel_name
            channel_data['channel_name'] = channel_name
            
            # 其他字段
            channel_data.update({
                'description': (about_renderer.get('description', '') or 
                              about_renderer.get('description', {}).get('simpleText', ''))[:1000],
                'country': (about_renderer.get('country', '') or 
                          about_renderer.get('country', {}).get('simpleText', ''))[:50],
                'subscriber_count': self._convert_subscriber_count(about_renderer),
                'view_count': self._convert_view_count(about_renderer),
                'joined_date': self._convert_joined_date(about_renderer),
                'video_count': self._convert_video_count(about_renderer),
                'canonical_url': self._process_canonical_url(about_renderer)
            })
            
            return channel_data
            
        except Exception as e:
            self.logger.log(f"提取频道数据时出错: {str(e)}", 'ERROR')
            return None
    
    def _convert_subscriber_count(self, about_renderer: Dict[str, Any]) -> int:
        """转换订阅者数量"""
        subscriber_text = about_renderer.get('subscriberCountText', '') or \
                        about_renderer.get('subscriberCount', '')
        try:
            if isinstance(subscriber_text, dict):
                subscriber_text = subscriber_text.get('simpleText', '0')
            subscriber_str = str(subscriber_text)
            
            if 'subscribers' in subscriber_str:
                num_str = subscriber_str.replace(' subscribers', '').replace(',', '')
                if 'K' in num_str:
                    return int(float(num_str.replace('K', '')) * 1000)
                elif 'M' in num_str:
                    return int(float(num_str.replace('M', '')) * 1000000)
                else:
                    return int(num_str)
            else:
                return int(subscriber_str.replace(',', ''))
        except (ValueError, TypeError, AttributeError) as e:
            self.logger.log(f"转换订阅者数量出错: {str(e)}, 原始文本: {subscriber_text}")
            return 0
    
    def _convert_view_count(self, about_renderer: Dict[str, Any]) -> int:
        """转换观看次数"""
        view_text = about_renderer.get('viewCountText', '') or \
                   about_renderer.get('viewCount', '')
        try:
            if isinstance(view_text, dict):
                view_text = view_text.get('simpleText', '0')
            view_str = str(view_text)
            
            if 'views' in view_str:
                return int(view_str.replace(' views', '').replace(',', ''))
            else:
                return int(view_str.replace(',', ''))
        except (ValueError, TypeError, AttributeError) as e:
            self.logger.log(f"转换观看次数出错: {str(e)}, 原始文本: {view_text}")
            return 0
    
    def _convert_joined_date(self, about_renderer: Dict[str, Any]) -> Optional[str]:
        """转换加入日期"""
        joined_text = None
        if isinstance(about_renderer.get('joinedDateText', {}), dict):
            joined_text = about_renderer.get('joinedDateText', {}).get('content', '')
        else:
            joined_text = about_renderer.get('joinedDateText', '')
        
        if joined_text:
            try:
                joined_str = str(joined_text)
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
                        return f"{year}-{month}-{day}"
            except (IndexError, ValueError, AttributeError) as e:
                self.logger.log(f"转换加入日期出错: {str(e)}, 原始文本: {joined_text}")
        return None
    
    def _convert_video_count(self, about_renderer: Dict[str, Any]) -> int:
        """转换视频数量"""
        video_text = about_renderer.get('videoCountText', '') or \
                    about_renderer.get('videoCount', '')
        try:
            if isinstance(video_text, dict):
                video_text = video_text.get('simpleText', '0')
            video_str = str(video_text)
            
            if ' videos' in video_str:
                return int(video_str.replace(' videos', '').replace(',', ''))
            else:
                return int(video_str.replace(',', ''))
        except (ValueError, TypeError, AttributeError) as e:
            self.logger.log(f"转换视频数量出错: {str(e)}, 原始文本: {video_text}")
            return 0
    
    def _process_canonical_url(self, about_renderer: Dict[str, Any]) -> str:
        """处理规范URL"""
        canonical_url = about_renderer.get('canonicalChannelUrl', '') or \
                       about_renderer.get('canonicalBaseUrl', '')
        if canonical_url.startswith(('http://www.youtube.com', 'https://www.youtube.com')):
            canonical_url = canonical_url.split('youtube.com')[1]
        return canonical_url

    def parse_channel_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        解析频道信息
        Args:
            data: 频道数据
        Returns:
            解析后的频道信息
        """
        try:
            if not data:
                self.logger.log("没有频道数据可解析", 'WARNING')
                return {}
            
            # 提取基本信息
            channel_info = {
                'channel_id': data.get('id'),
                'title': data.get('snippet', {}).get('title'),
                'description': data.get('snippet', {}).get('description'),
                'published_at': data.get('snippet', {}).get('publishedAt'),
                'thumbnails': data.get('snippet', {}).get('thumbnails', {}),
                'statistics': data.get('statistics', {})
            }
            
            return channel_info
        except Exception as e:
            self.logger.log(f"解析频道信息时出错: {str(e)}", 'ERROR')
            return {}
    
    def parse_video_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        解析视频信息
        Args:
            data: 视频数据
        Returns:
            解析后的视频信息
        """
        try:
            if not data:
                self.logger.log("没有视频数据可解析", 'WARNING')
                return {}
            
            # 提取基本信息
            video_info = {
                'video_id': data.get('id'),
                'title': data.get('snippet', {}).get('title'),
                'description': data.get('snippet', {}).get('description'),
                'published_at': data.get('snippet', {}).get('publishedAt'),
                'thumbnails': data.get('snippet', {}).get('thumbnails', {}),
                'statistics': data.get('statistics', {})
            }
            
            return video_info
        except Exception as e:
            self.logger.log(f"解析视频信息时出错: {str(e)}", 'ERROR')
            return {}
    
    def parse_playlist_info(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        解析播放列表信息
        Args:
            data: 播放列表数据
        Returns:
            解析后的播放列表信息
        """
        try:
            if not data:
                self.logger.log("没有播放列表数据可解析", 'WARNING')
                return {}
            
            # 提取基本信息
            playlist_info = {
                'playlist_id': data.get('id'),
                'title': data.get('snippet', {}).get('title'),
                'description': data.get('snippet', {}).get('description'),
                'published_at': data.get('snippet', {}).get('publishedAt'),
                'thumbnails': data.get('snippet', {}).get('thumbnails', {}),
                'item_count': data.get('contentDetails', {}).get('itemCount')
            }
            
            return playlist_info
        except Exception as e:
            self.logger.log(f"解析播放列表信息时出错: {str(e)}", 'ERROR')
            return {} 
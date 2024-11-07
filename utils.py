import base64
import brotli
import re
import json
import time
import os
import csv

class Utils:
    """
    通用工具类，包含各种辅助方法
    """
    
    @staticmethod
    def process_response_content(response):
        """
        处理响应内容的通用函数（处理编码和压缩）
        Args:
            response: 响应对象，包含content和headers信息
        Returns:
            str: 处理后的响应文本
        """
        response_text = response['content']['text']
        
        # 检查是否是base64编码
        if response['content'].get('encoding') == 'base64':
            response_text = base64.b64decode(response_text)
        
        # 检查是否是br压缩
        try:
            if any(h['name'].lower() == 'content-encoding' and 'br' in h['value'].lower() 
                  for h in response['headers']):
                response_text = brotli.decompress(response_text)
        except Exception as e:
            print(f"解压响应内容时出错: {str(e)}")
        
        # 如果是bytes，转换为字符串
        if isinstance(response_text, bytes):
            response_text = response_text.decode('utf-8')
        
        return response_text

    @staticmethod
    def save_video_data(video_data):
        """
        将视频数据保存到CSV文件
        Args:
            video_data: 视频数据字典或列表
        """
        output_file = "youtube_videos.csv"
        
        try:
            # 转换时间格式和观看次数
            if isinstance(video_data, list):
                for item in video_data:
                    if 'published_time_text' in item:
                        item['published_time_text'] = Utils.convert_relative_time(item['published_time_text'])
                    if 'viewCountText' in item:
                        item['viewCountText'] = Utils.convert_view_count(item['viewCountText'])
            else:
                if 'published_time_text' in video_data:
                    video_data['published_time_text'] = Utils.convert_relative_time(video_data['published_time_text'])
                if 'viewCountText' in video_data:
                    video_data['viewCountText'] = Utils.convert_view_count(video_data['viewCountText'])
            
            # 确定表头字段
            if isinstance(video_data, list):
                fieldnames = video_data[0].keys() if video_data else []
            else:
                fieldnames = video_data.keys()
            
            # 检查文件是否存在，不存在则创建并写入表头
            file_exists = os.path.isfile(output_file)
            
            with open(output_file, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                
                # 如果文件不存在，写入表头
                if not file_exists:
                    writer.writeheader()
                
                # 写入数据
                if isinstance(video_data, list):
                    writer.writerows(video_data)
                    print(f"保存了 {len(video_data)} 条视频数据到CSV文件")
                else:
                    writer.writerow(video_data)
                    print(f"保存了视频 {video_data.get('videoId')} 的数据到CSV文件")
                
        except Exception as e:
            print(f"保存视频数据到CSV文件时出错: {str(e)}")

    @staticmethod
    def analyze_and_store_json_response_first(json_data):
        """
        分析并存储JSON响应数据
        Args:
            json_data: JSON格式的响应数据（已解析为Python对象）
        """
        print("\n分析JSON响应...")
        
        # 获取有效内容
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
        
        print(f"\n找到 {len(effective_contents)} 个视频内容")
        
        for item in effective_contents:
            videoRenderer = item.get('videoRenderer', {})
            if not videoRenderer:
                continue
                
            video_id = videoRenderer.get('videoId', '')
            if not video_id:
                continue
                
            video_data = {
                'videoId': video_id,
                'video_url': f'https://www.youtube.com/watch?v={video_id}',
                'cover_url': f'https://i.ytimg.com/vi/{video_id}/frame0.jpg',
                'title': videoRenderer.get('title', {}).get('runs', [{}])[0].get('text', ''),
                'snippet_text': ''.join(run.get('text', '') for snippet in videoRenderer.get('detailedMetadataSnippets', []) 
                                        for run in snippet.get('snippetText', {}).get('runs', [])),
                'channel_name': videoRenderer.get('longBylineText', {}).get('runs', [{}])[0].get('text', ''),
                'channel_id': videoRenderer.get('longBylineText', {}).get('runs', [{}])[0].get('navigationEndpoint', {}).get('browseEndpoint', {}).get('browseId', ''),
                'channel_url': f"https://www.youtube.com{videoRenderer.get('longBylineText', {}).get('runs', [{}])[0].get('navigationEndpoint', {}).get('browseEndpoint', {}).get('canonicalBaseUrl', '')}",
                'published_time_text': videoRenderer.get('publishedTimeText', {}).get('simpleText', ''),
                'lengthText': videoRenderer.get('lengthText', {}).get('simpleText', ''),
                'viewCountText': videoRenderer.get('viewCountText', {}).get('simpleText', ''),
                'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S')
            }
            
            results.append(video_data)
            print(f"\n视频 {video_id} 的信息:")
            for key, value in video_data.items():
                print(f"{key}: {value}")
        
        # 批量保存数据
        if results:
            Utils.save_video_data(results)
        
        print(f"\n总共解析了 {len(results)} 个视频的数据")
        return results
    
    @staticmethod
    def analyze_and_store_json_response_else(json_data):
        """
        分析并存储JSON响应数据
        Args:
            json_data: JSON格式的响应数据（已解析为Python对象）
        """
        print("\n分析JSON响应...")
        
        # 获取视频内容
        try:
            contents = json_data['onResponseReceivedCommands'][0] \
                ['appendContinuationItemsAction']['continuationItems'][0] \
                ['itemSectionRenderer']['contents']
            print(f"\n找到 {len(contents)} 个视频内容")
        except (KeyError, IndexError):
            print("未找到视频内容")
            return []

        results = []
        for item in contents:
            try:
                renderer = item['videoRenderer']
                video_id = renderer['videoId']
                
                video_data = {
                    'videoId': video_id,
                    'video_url': f'https://www.youtube.com/watch?v={video_id}',
                    'cover_url': f'https://i.ytimg.com/vi/{video_id}/frame0.jpg',
                    'title': renderer['title']['runs'][0]['text'],
                    'snippet_text': ''.join(
                        run['text'] 
                        for snippet in renderer.get('detailedMetadataSnippets', []) 
                        for run in snippet.get('snippetText', {}).get('runs', [])
                    ),
                    'channel_name': renderer['longBylineText']['runs'][0]['text'],
                    'channel_id': renderer['longBylineText']['runs'][0] \
                        ['navigationEndpoint']['browseEndpoint']['browseId'],
                    'channel_url': f"https://www.youtube.com" + \
                        renderer['longBylineText']['runs'][0] \
                        ['navigationEndpoint']['browseEndpoint']['canonicalBaseUrl'],
                    'published_time_text': renderer['publishedTimeText']['simpleText'],
                    'lengthText': renderer['lengthText']['simpleText'],
                    'viewCountText': renderer['viewCountText']['simpleText'],
                    'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S')
                }
                
                results.append(video_data)
                print(f"\n视频 {video_id} 的信息:")
                for key, value in video_data.items():
                    print(f"{key}: {value}")
                
            except Exception as e:
                print(f"处理视频时出错: {str(e)}")
                continue
        
        # 批量保存数据
        if results:
            Utils.save_video_data(results)
        
        print(f"\n总共解析了 {len(results)} 个视频的数据")
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
            print(f"已保存响应JSON到文件: {filename}")
        except Exception as e:
            print(f"保存响应JSON时出错: {str(e)}")

    @staticmethod
    def analyze_and_store_shorts_json_response(json_data):
        """
        分析并存储短视频JSON响应数据
        Args:
            json_data: JSON格式的响应数据（已解析为Python对象）
        """
        print("\n分析短视频JSON响应...")
        
        # 获取视频内容列表
        try:
            contents = json_data.get('onResponseReceivedActions', [])[0] \
                        .get('appendContinuationItemsAction', {}) \
                        .get('continuationItems', [])
            
            if not contents:
                print("未找到视频内容")
                return []
                
            print(f"\n找到 {len(contents)} 个内容项")
        except Exception as e:
            print(f"获取内容列表时出错: {str(e)}")
            return []

        results = []
        
        for item in contents:
            try:
                # 跳过continuation项
                if 'continuationItemRenderer' in item:
                    continue
                    
                # 获取视频信息
                video_content = item.get('richItemRenderer', {}).get('content', {}) \
                                .get('shortsLockupViewModel', {})
                
                if not video_content:
                    continue
                
                # 从entityId中提取videoId (格式: "shorts-shelf-item-{videoId}")
                entity_id = video_content.get('entityId', '')
                video_id = entity_id.replace('shorts-shelf-item-', '')
                
                if not video_id:
                    continue
                    
                # 提取视频信息
                video_data = {
                    'videoId': video_id,
                    'video_url': f'https://www.youtube.com/shorts/{video_id}',
                    'cover_url': f'https://i.ytimg.com/vi/{video_id}/frame0.jpg',
                    'title': video_content.get('accessibilityText', ''),
                    'view_count': video_content.get('overlayMetadata', {}) \
                                 .get('secondaryText', {}).get('content', ''),
                    'crawl_time': time.strftime('%Y-%m-%d %H:%M:%S')
                }
                
                results.append(video_data)
                print(f"\n视频 {video_id} 的信息:")
                for key, value in video_data.items():
                    print(f"{key}: {value}")
                    
            except Exception as e:
                print(f"处理视频项时出错: {str(e)}")
                continue
        
        # 批量保存数据
        if results:
            Utils.save_video_data(results)
        
        print(f"\n总共解析了 {len(results)} 个视频的数据")
        return results

    @staticmethod
    def convert_relative_time(relative_time_str):
        """
        将相对时间字符串转换为具体时间
        Args:
            relative_time_str: 相对时间字符串，如"1个月前"、"2周前"、"3天前"等
        Returns:
            str: 转换后的时间字符串，格式为'%Y-%m-%d %H:%M:%S'
        """
        try:
            # 获取当前时间
            current_time = time.time()
            
            # 解析数字和单位
            if not relative_time_str:
                return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))
                
            # 提取数字和单位
            import re
            match = re.match(r'(\d+)?(.*?)前', relative_time_str)
            if not match:
                return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))
                
            number = int(match.group(1)) if match.group(1) else 1
            unit = match.group(2)
            
            # 转换单位到秒
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
            
            # 计算具体时间
            target_time = current_time - seconds
            
            # 转换为指定格式
            return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(target_time))
            
        except Exception as e:
            print(f"时间转换出错: {str(e)}")
            return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(current_time))

    @staticmethod
    def convert_view_count(view_count_str):
        """
        将观看次数字符串转换为整数
        Args:
            view_count_str: 观看次数字符串，如"102,717次观看"
        Returns:
            int: 转换后的整数
        """
        try:
            if not view_count_str:
                return 0
                
            # 移除"次观看"并替换逗号
            number_str = view_count_str.replace('次观看', '').replace(',', '')
            
            # 转换为整数
            return int(number_str)
            
        except Exception as e:
            print(f"观看次数转换出错: {str(e)}")
            return 0
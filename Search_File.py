import json
import os
import re
from bs4 import BeautifulSoup

def extract_youtube_search(filename):
    # 读取文本文件
    with open(filename, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # 查找所有Response部分
    responses = []
    response_sections = content.split("=== Search Request ===")
    print(f"找到 {len(response_sections)-1} 个响应部分")  # 调试信息
    
    for section in response_sections[1:]:  # 跳过第一个空部分
        try:
            # 提取Response部分
            if "Response:" in section:
                print("找到Response部分")  # 调试信息
                response_text = section.split("Response:\n")[1].split("\n----------------")[0]
                print("提取的响应文本长度:", len(response_text))  # 调试信息
                response_json = json.loads(response_text)
                print("成功解析JSON")  # 调试信息
                
                # 打印JSON的顶层键
                print("JSON顶层键:", list(response_json.keys()))  # 调试信息
                
                # 从JSON中提取视频信息
                if "contents" in response_json:
                    print("找到contents字段")  # 调试信息
                    for content_section in response_json["contents"]:
                        print("处理content_section, 键:", list(content_section.keys()))  # 调试信息
                        if "itemSectionRenderer" in content_section:
                            print("找到itemSectionRenderer")  # 调试信息
                            items = content_section["itemSectionRenderer"]["contents"]
                            print(f"找到 {len(items)} 个items")  # 调试信息
                            for item in items:
                                print("处理item, 键:", list(item.keys()))  # 调试信息
                                if "videoRenderer" in item:
                                    print("找到videoRenderer")  # 调试信息
                                    video = item["videoRenderer"]
                                    video_data = {
                                        'title': video["title"]["runs"][0]["text"] if "title" in video else '',
                                        'video_url': f"https://www.youtube.com/watch?v={video['videoId']}" if 'videoId' in video else '',
                                        'channel_name': video["longBylineText"]["runs"][0]["text"] if "longBylineText" in video else '',
                                        'channel_url': f"https://www.youtube.com{video['longBylineText']['runs'][0]['navigationEndpoint']['commandMetadata']['webCommandMetadata']['url']}" if "longBylineText" in video else '',
                                        'view_count': video["viewCountText"]["simpleText"] if "viewCountText" in video else '',
                                        'published_time': video["publishedTimeText"]["simpleText"] if "publishedTimeText" in video else ''
                                    }
                                    responses.append(video_data)
                                    print(f"找到视频: {video_data['title']}")  # 调试信息
        except Exception as e:
            print(f"处理响应内容时出错: {str(e)}")
            # 打印更详细的错误信息
            import traceback
            print(traceback.format_exc())
            continue
            
    return responses

def main():
    # 获取当前目录下最新的搜索请求文件
    files = [f for f in os.listdir('.') if (f.startswith('search_request_') or f.startswith('search_requests_')) and f.endswith('.txt')]
    if not files:
        print("未找到搜索请求文件")
        return
        
    latest_file = max(files)
    print(f"正在处理文件: {latest_file}")
    
    # 提取视频信息
    results = extract_youtube_search(latest_file)
    
    # 打印结果
    print(f"\n找到 {len(results)} 个视频:")
    for video in results:
        print(f"\n标题: {video['title']}")
        print(f"频道: {video['channel_name']}")
        print(f"频道链接: {video['channel_url']}")
        print(f"视频链接: {video['video_url']}")
        print(f"观看次数: {video['view_count']}")
        print(f"发布时间: {video['published_time']}")
        print("-" * 50)

if __name__ == "__main__":
    main()
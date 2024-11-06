import json
import os
import re
from bs4 import BeautifulSoup

def extract_youtube_search(filename):
    # 读取文本文件
    with open(filename, 'r', encoding='utf-8') as file:
        content = file.read()
    
    # 提取 Page Source 部分
    match = re.search(r'Page Source:\n(.*?)\nResponse Headers:', content, re.DOTALL)
    if not match:
        print("无法在文件中找到页面源代码")
        return []
        
    html_content = match.group(1)
    
    # 使用 BeautifulSoup 解析 HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    results = []
    
    # 查找视频项
    video_items = soup.find_all('div', {'id': 'dismissible', 'class': 'style-scope ytd-video-renderer'})
    
    for item in video_items:
        try:
            # 查找视频链接和标题
            video_title_elem = item.find('a', {'id': 'video-title'})
            video_url = video_title_elem.get('href', '') if video_title_elem else ''
            title = video_title_elem.get('title', '') if video_title_elem else ''
            
            # 查找频道信息
            channel_elem = item.find('a', {'id': 'channel-thumbnail'})
            channel_url = channel_elem.get('href', '') if channel_elem else ''
            channel_name = item.find('yt-formatted-string', {'id': 'text'}).find('a').text if item.find('yt-formatted-string', {'id': 'text'}) else ''
            
            # 查找观看次数和发布时间
            metadata_line = item.find('div', {'id': 'metadata-line'})
            view_count = metadata_line.find_all('span', {'class': 'inline-metadata-item'})[0].text if metadata_line else ''
            published_time = metadata_line.find_all('span', {'class': 'inline-metadata-item'})[1].text if metadata_line else ''
            
            video_data = {
                'title': title,
                'video_url': f"https://www.youtube.com{video_url}" if video_url else '',
                'channel_name': channel_name,
                'channel_url': f"https://www.youtube.com{channel_url}" if channel_url else '',
                'view_count': view_count,
                'published_time': published_time
            }
            
            results.append(video_data)
            
        except Exception as e:
            print(f"处理视频项时出错: {str(e)}")
            continue
            
    return results

def main():
    # 修改文件名匹配模式，同时支持带s和不带s的情况
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
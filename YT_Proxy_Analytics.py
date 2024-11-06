from browsermobproxy import Server
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import json
import base64
import brotli

def scroll_and_analyze(driver, proxy, output_file, max_scrolls=5):
    processed_entries = set()
    search_request_count = 0
    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    print("初始页面高度:", last_height)
    
    scroll_count = 0
    
    while scroll_count < max_scrolls:
        try:
            driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            print(f"\n执行第 {scroll_count + 1} 次滚动")
            time.sleep(2)
            
            with open(output_file, 'a', encoding='utf-8') as f:
                for entry in proxy.har['log']['entries']:
                    request_url = entry['request']['url']
                    entry_id = f"{request_url}_{entry['startedDateTime']}"
                    
                    if (entry_id not in processed_entries and 
                        ('www.youtube.com/results?search_query' in request_url or 
                         'www.youtube.com/youtubei/v1/search' in request_url)):
                        
                        search_request_count += 1
                        print(f"\n找到第 {search_request_count} 个搜索请求: {request_url}")
                        
                        # 保存请求基本信息
                        f.write(f"\n=== Search Request #{search_request_count} ===\n")
                        f.write(f"URL: {request_url}\n")
                        f.write(f"Method: {entry['request']['method']}\n")
                        f.write(f"Time: {time.strftime('%H:%M:%S')}\n")
                        
                        # 获取响应内容
                        response = entry['response']
                        if response['content'].get('text'):
                            try:
                                # 获取响应文本
                                response_text = response['content']['text']
                                
                                # 检查是否是base64编码
                                if response['content'].get('encoding') == 'base64':
                                    response_text = base64.b64decode(response_text)
                                
                                # 检查是否是br压缩
                                try:
                                    if any(h['name'].lower() == 'content-encoding' and 'br' in h['value'].lower() 
                                          for h in entry['response']['headers']):
                                        response_text = brotli.decompress(response_text)
                                except Exception as e:
                                    print(f"解压响应内容时出错: {str(e)}")
                                
                                # 如果是bytes，转换为字符串
                                if isinstance(response_text, bytes):
                                    response_text = response_text.decode('utf-8')
                                
                                # 尝试解析JSON
                                try:
                                    response_json = json.loads(response_text)
                                    f.write("\nResponse:\n")
                                    f.write(json.dumps(response_json, indent=2, ensure_ascii=False))
                                except json.JSONDecodeError:
                                    f.write("\nResponse (raw):\n")
                                    f.write(response_text)
                                    
                            except Exception as e:
                                print(f"处理响应内容时出错: {str(e)}")
                        else:
                            f.write("\nNo response content available\n")
                        
                        f.write("\n" + "-" * 80 + "\n")
                        processed_entries.add(entry_id)
            
            new_height = driver.execute_script("return document.documentElement.scrollHeight")
            if new_height == last_height:
                print("已到达页面底部")
                break
            last_height = new_height
            scroll_count += 1
            print(f"页面已滚动 {scroll_count} 次，当前高度: {new_height}")
            
        except Exception as e:
            print(f"滚动过程中出错: {str(e)}")
            break
    
    print(f"完成滚动，共执行 {scroll_count} 次")
    print(f"总共找到 {search_request_count} 个搜索请求")

try:
    print("启动代理服务器...")
    server = Server(r"C:\Program Files\browsermob-proxy-2.1.4\bin\browsermob-proxy.bat")
    server.start()
    print("代理服务器启动成功")
    
    proxy = server.create_proxy()
    print(f"创建代理成功，地址: {proxy.proxy}")
    
    # 配置Chrome选项
    chrome_options = Options()
    chrome_options.add_argument(f'--proxy-server={proxy.proxy}')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--ignore-ssl-errors')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    chrome_options.set_capability('acceptInsecureCerts', True)
    
    print("初始化Chrome浏览器...")
    driver = webdriver.Chrome(options=chrome_options)
    print("Chrome浏览器初始化成功")
    
    # 开始记录网络流量，修改捕获选项
    print("开始记录网络流量...")
    proxy.new_har("youtube", options={
        'captureHeaders': True,
        'captureContent': True,
        'captureBinaryContent': True,
        'captureEncoding': True,
        'captureMimeTypes': ['text/plain', 'application/json', 'application/javascript', 'text/html'],
        'captureCompression': True
    })
    
    # 访问YouTube搜索页面
    search_query = "baby+fashion+show"
    print(f"访问YouTube搜索页面: {search_query}")
    driver.get(f"https://www.youtube.com/results?search_query={search_query}")
    print("等待页面加载...")
    time.sleep(3)  # 等待初始页面加载
    
    # 创建输出文件
    output_file = f"search_requests_{time.strftime('%Y%m%d_%H%M%S')}.txt"
    print(f"创建输出文件: {output_file}")
    
    # 执行滚动和分析
    print("开始执行滚动和分析...")
    scroll_and_analyze(driver, proxy, output_file)
    
    print(f"所有响应内容已保存到文件：{output_file}")
    
except Exception as e:
    print(f"程序执行出错: {str(e)}")
    
finally:
    print("清理资源...")
    if 'server' in locals():
        server.stop()
        print("代理服务器已停止")
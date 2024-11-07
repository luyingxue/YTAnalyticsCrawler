from browsermobproxy import Server
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import json
from utils import Utils

def scroll_and_analyze(driver, proxy, max_scrolls=2):
    processed_entries = set()
    request_count = 0
    last_height = driver.execute_script("return document.documentElement.scrollHeight")
    print("初始页面高度:", last_height)
    
    # 获取当前页面URL并判断类型
    current_url = driver.current_url
    print(f"当前页面URL: {current_url}")
    
    # 定义需要捕获的请求URL模式
    if 'youtube.com/results' in current_url:
        # 搜索结果页面
        target_url = 'www.youtube.com/youtubei/v1/search'
        print("检测到搜索结果页面，将捕获搜索API请求")
    elif 'youtube.com/hashtag' in current_url:
        # 话题标签页面
        target_url = 'www.youtube.com/youtubei/v1/browse'
        print("检测到话题标签页面，将捕获浏览API请求")
    else:
        # 其他页面，可以继续添加其他类型
        print(f"未识别的页面类型: {current_url}")
        target_url = 'www.youtube.com/youtubei/v1/search'  # 默认使用搜索API
    
    scroll_count = 0
    while scroll_count < max_scrolls:
        try:
            driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
            print(f"\n执行第 {scroll_count + 1} 次滚动")
            time.sleep(2)
            
            for entry in proxy.har['log']['entries']:
                request_url = entry['request']['url']
                entry_id = f"{request_url}_{entry['startedDateTime']}"
                
                if entry_id not in processed_entries and target_url in request_url:
                    request_count += 1
                    print(f"\n=== API Request #{request_count} ===")
                    print(f"URL: {request_url}")
                    print(f"Method: {entry['request']['method']}")
                    print(f"Time: {time.strftime('%H:%M:%S')}")
                    
                    # 获取响应内容
                    response = entry['response']
                    if response['content'].get('text'):
                        try:
                            # 使用工具类处理响应内容
                            response_text = Utils.process_response_content(response)
                            
                            # 解析JSON响应
                            response_json = json.loads(response_text)
                            
                            # 保存原始响应JSON
                            # Utils.save_response_json(response_json, request_count, is_initial=(request_count == 1))
                            
                            # 根据页面类型选择不同的分析方法
                            if 'youtube.com/hashtag' in current_url:
                                Utils.analyze_and_store_shorts_json_response(response_json)
                            else:
                                if request_count == 1:
                                    Utils.analyze_and_store_json_response_first(response_json)
                                else:
                                    Utils.analyze_and_store_json_response_else(response_json)
                                    
                        except json.JSONDecodeError as e:
                            print(f"JSON解析错误: {str(e)}")
                        except Exception as e:
                            print(f"处理API响应时出错: {str(e)}")
                    else:
                        print("No response content available")
                        
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
    print(f"总共找到 {request_count} 个API请求")

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
    # driver.get(f"https://www.youtube.com/hashtag/kidsfashion/shorts")
    print("等待页面加载...")
    time.sleep(3)  # 等待初始页面加载
    
    # 判断当前URL类型
    current_url = driver.current_url
    if 'youtube.com/hashtag' not in current_url:
        # 只有在非hashtag页面才点击Shorts按钮
        try:
            shorts_button = driver.find_element("xpath", 
                "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-search/div[1]/div/ytd-search-header-renderer/div[1]/yt-chip-cloud-renderer/div/div[2]/iron-selector/yt-chip-cloud-chip-renderer[2]/yt-formatted-string")
            print("找到 Shorts 按钮")
            
            if shorts_button.text == "Shorts":
                shorts_button.click()
                print("已点击 Shorts 按钮")
                time.sleep(3)  # 等待筛选结果加载
            else:
                print(f"按钮文字不匹配，期望 'Shorts'，实际是 '{shorts_button.text}'")
        except Exception as e:
            print(f"点击 Shorts 按钮时出错: {str(e)}")
    else:
        print("当前为hashtag页面，无需点击Shorts按钮")
    
    # 执行滚动和分析
    print("开始执行滚动和分析...")
    scroll_and_analyze(driver, proxy)
    
    print("\n任务完成,准备清理资源...")
    
except Exception as e:
    print(f"程序执行出错: {str(e)}")
    
finally:
    print("清理资源...")
    if 'driver' in locals():
        driver.quit()
        print("浏览器已关闭")
    if 'server' in locals():
        server.stop()
        print("代理服务器已停止")
    print("程序结束")
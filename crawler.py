from browsermobproxy import Server
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
import json
from utils import Utils
from db_manager import DBManager
import logging
from log_manager import LogManager

class YoutubeCrawler:
    def __init__(self, proxy_path=r"C:\Program Files\browsermob-proxy-2.1.4\bin\browsermob-proxy.bat", worker_id=None):
        """
        初始化爬虫
        Args:
            proxy_path: BrowserMob代理路径
            worker_id: 工作进程ID，用于日志区分
        """
        self.proxy_path = proxy_path
        self.server = None
        self.proxy = None
        self.driver = None
        self.worker_id = worker_id
        self.logger = LogManager().get_logger()
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message, self.worker_id)

    def setup(self):
        """设置代理和浏览器"""
        try:
            self.log("启动代理服务器...")
            
            # 修改端口分配逻辑
            base_port = 8080
            worker_id = int(str(self.worker_id).replace('Worker ', ''))
            
            # 尝试不同的端口
            max_port_attempts = 5
            for port_offset in range(max_port_attempts):
                try:
                    port = base_port + worker_id + (port_offset * 100)  # 使用更大的间隔
                    self.log(f"尝试使用端口: {port}")
                    
                    self.server = Server(self.proxy_path, {'port': port})
                    self.server.start()
                    self.log("代理服务器启动成功")
                    
                    self.proxy = self.server.create_proxy()
                    self.log(f"创建代理成功，地址: {self.proxy.proxy}")
                    break
                    
                except Exception as e:
                    if port_offset == max_port_attempts - 1:  # 最后一次尝试
                        raise
                    self.log(f"端口 {port} 启动失败: {str(e)}, 尝试下一个端口")
                    if self.server:
                        try:
                            self.server.stop()
                        except:
                            pass
                    time.sleep(1)
            
            # 配置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument(f'--proxy-server={self.proxy.proxy}')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--ignore-ssl-errors')
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            chrome_options.set_capability('acceptInsecureCerts', True)
            
            self.log("初始化Chrome浏览器...")
            self.driver = webdriver.Chrome(options=chrome_options)
            self.log("Chrome浏览器初始化成功")
            
            # 开始记录网络流量
            self.proxy.new_har("youtube", options={
                'captureHeaders': True,
                'captureContent': True,
                'captureBinaryContent': True,
                'captureEncoding': True,
                'captureMimeTypes': ['text/plain', 'application/json', 'application/javascript', 'text/html'],
                'captureCompression': True
            })
            
        except Exception as e:
            self.log(f"设置代理和浏览器时出错: {str(e)}", 'ERROR')
            self.cleanup()
            raise
            
    def cleanup(self):
        """清理资源"""
        if self.driver:
            self.driver.quit()
            self.log("浏览器已关闭")
        if self.server:
            self.server.stop()
            self.log("代理服务器已停止")
            
    def process_url(self, url_data):
        """处理单个URL的爬取任务"""
        try:
            target_url = url_data['url']
            self.log(f"处理URL: {target_url} (关键词: {url_data['key_words']})")
            
            # 访问页面
            self.driver.get(target_url)
            time.sleep(3)
            
            # 点击Shorts按钮
            self._click_shorts_button()
            
            # 执行滚动和分析
            self.log("开始执行滚动和分析...")
            self._scroll_and_analyze()
            
        except Exception as e:
            self.log(f"处理URL时出错: {str(e)}")
            raise
            
    def _click_shorts_button(self):
        """点击Shorts按钮"""
        try:
            shorts_button = self.driver.find_element("xpath", 
                "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-search/div[1]/div/ytd-search-header-renderer/div[1]/yt-chip-cloud-renderer/div/div[2]/iron-selector/yt-chip-cloud-chip-renderer[2]/yt-formatted-string")
            
            if shorts_button.text == "Shorts":
                shorts_button.click()
                self.log("已点击 Shorts 按钮")
                time.sleep(3)
            else:
                self.log(f"按钮文字不匹配，期望 'Shorts'，实际是 '{shorts_button.text}'")
        except Exception as e:
            self.log(f"点击 Shorts 按钮时出错: {str(e)}")
            
    def _scroll_and_analyze(self):
        """执行滚动和分析，直到无法继续滚动"""
        processed_entries = set()
        request_count = 0
        last_height = self.driver.execute_script("return document.documentElement.scrollHeight")
        self.log(f"初始页面高度: {last_height}")
        
        # 定义需要捕获的请求URL模式
        target_url = 'www.youtube.com/youtubei/v1/search'
        self.log("检测到搜索结果页面，将捕获搜索API请求")
        
        scroll_count = 0
        no_change_count = 0  # 添加计数器跟踪页面高度未变化的次数
        
        while True:  # 移除max_scrolls限制
            try:
                self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                self.log(f"\n执行第 {scroll_count + 1} 次滚动")
                time.sleep(2)
                
                # 处理网络请求
                for entry in self.proxy.har['log']['entries']:
                    request_url = entry['request']['url']
                    entry_id = f"{request_url}_{entry['startedDateTime']}"
                    
                    if entry_id not in processed_entries and target_url in request_url:
                        request_count += 1
                        self.log(f"\n=== API Request #{request_count} ===")
                        self.log(f"URL: {request_url}")
                        self.log(f"Method: {entry['request']['method']}")
                        self.log(f"Time: {time.strftime('%H:%M:%S')}")
                        
                        # 获取响应内容
                        response = entry['response']
                        if response['content'].get('text'):
                            try:
                                response_text = Utils.process_response_content(response)
                                response_json = json.loads(response_text)
                                
                                # 处理响应数据
                                if request_count == 1:
                                    Utils.analyze_and_store_json_response_first(response_json)
                                else:
                                    Utils.analyze_and_store_json_response_else(response_json)
                                    
                            except json.JSONDecodeError as e:
                                self.log(f"JSON解析错误: {str(e)}", 'ERROR')
                            except Exception as e:
                                self.log(f"处理API响应时出错: {str(e)}", 'ERROR')
                        else:
                            self.log("No response content available")
                            
                        processed_entries.add(entry_id)
                
                new_height = self.driver.execute_script("return document.documentElement.scrollHeight")
                if new_height == last_height:
                    no_change_count += 1
                    self.log(f"页面高度未变化，计数: {no_change_count}")
                    if no_change_count >= 3:  # 连续3次高度未变化则认为到达底部
                        self.log("已到达页面底部")
                        break
                else:
                    no_change_count = 0  # 高度有变化，重置计数器
                    
                last_height = new_height
                scroll_count += 1
                self.log(f"页面已滚动 {scroll_count} 次，当前高度: {new_height}")
                
            except Exception as e:
                self.log(f"滚动过程中出错: {str(e)}", 'ERROR')
                break
        
        self.log(f"完成滚动，共执行 {scroll_count} 次")
        self.log(f"总共找到 {request_count} 个API请求")
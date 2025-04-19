from browsermobproxy import Server
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
from src.utils import ResponseProcessor, FileHandler
from src.services import VideoService
from src.utils.logger import Logger
from src.utils.youtube_parser import YouTubeParser
import logging
from typing import Dict, Any

class VideoCrawler:
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
        self.logger = Logger().get_logger(f'Crawler-{worker_id}' if worker_id else 'Crawler')
        self.video_service = VideoService()
        self.response_processor = ResponseProcessor()
        self.file_handler = FileHandler()
        self.youtube_parser = YouTubeParser()
        
    def log(self, message, level='INFO'):
        """输出日志"""
        # 将字符串日志级别转换为对应的整数常量
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        level_int = level_map.get(level, logging.INFO)
        self.logger.log(level_int, message)
        
    def setup(self):
        """设置爬虫环境"""
        try:
            # 启动BrowserMob代理
            self.server = Server(self.proxy_path)
            self.server.start()
            self.proxy = self.server.create_proxy()
            self.proxy.new_har("youtube")
            
            # 配置Chrome选项
            chrome_options = Options()
            chrome_options.add_argument('--proxy-server={0}'.format(self.proxy.proxy))
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--enable-unsafe-swiftshader')
            chrome_options.add_argument('--disable-notifications')
            chrome_options.add_argument('--disable-popup-blocking')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--ignore-ssl-errors')
            chrome_options.add_argument('--lang=zh-CN')
            chrome_options.add_argument('--start-maximized')
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # 启动Chrome浏览器
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            
            self.log("爬虫环境设置完成")
            return True
            
        except Exception as e:
            self.log(f"设置爬虫环境时出错: {str(e)}", 'ERROR')
            self.cleanup()
            return False
            
    def cleanup(self):
        """清理爬虫资源"""
        try:
            if self.driver:
                self.driver.quit()
                self.driver = None
                
            if self.proxy:
                self.proxy.close()
                self.proxy = None
                
            if self.server:
                self.server.stop()
                self.server = None
                
            self.log("爬虫资源已清理")
            
        except Exception as e:
            self.log(f"清理爬虫资源时出错: {str(e)}", 'ERROR')
            
    def process_url(self, url_data):
        """
        处理URL
        Args:
            url_data: URL数据，包含url和is_benchmark字段
        Returns:
            bool: 处理是否成功
        """
        try:
            url = url_data.get('url', '')
            is_benchmark = url_data.get('is_benchmark', False)
            
            if not url:
                self.log("URL为空，跳过处理")
                return False
                
            self.log(f"开始处理URL: {url}, is_benchmark={is_benchmark}")
            
            # 访问URL
            self.driver.get(url)
            time.sleep(5)  # 等待页面加载
            
            # 处理Shorts内容
            return self._process_shorts()
            
        except Exception as e:
            self.log(f"处理URL时出错: {str(e)}", 'ERROR')
            return False
            
    def _process_shorts(self):
        """处理Shorts内容：点击按钮并分析数据"""
        try:
            # 等待Shorts按钮出现
            time.sleep(3)
            shorts_button = WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.XPATH, "//yt-chip-cloud-chip-renderer[.//yt-formatted-string[contains(text(), 'Shorts')]]"))
            )
            
            # 开始监视网络请求
            self.log("开始监视网络请求")
            self.proxy.new_har("youtube", options={
                'captureHeaders': True,
                'captureContent': True,
                'captureBinaryContent': True,
                'captureEncoding': True,
                'urlPattern': '.*/youtubei/v1/search.*'  # 捕获所有youtubei/v1/search请求
            })
            
            # 点击Shorts按钮
            shorts_button.click()
            time.sleep(3)
            self.log("已点击Shorts按钮")
            
            # 初始化变量
            scroll_count = 0
            max_scrolls = 10
            request_count = 0
            is_initial = True
            processed_contents = set()  # 用于跟踪已处理的响应内容
            
            # 执行滚动操作
            self.log("开始执行页面滚动")
            while scroll_count < max_scrolls:
                # 使用更可靠的滚动方式
                self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                time.sleep(3)  # 增加等待时间，确保页面加载完成
                
                # 增加滚动计数
                scroll_count += 1
                self.log(f"已完成第 {scroll_count} 次滚动")
                
                # 获取当前的HAR日志
                entries = self.proxy.har['log']['entries']
                self.log(f"当前捕获到 {len(entries)} 个请求")
                
                # 只处理新的search请求
                for entry in entries:
                    request_url = entry['request']['url']
                    self.log(f"处理请求: {request_url}")
                    
                    # 检查是否是YouTube search API请求
                    if '/youtubei/v1/search' not in request_url:
                        self.log("不是YouTube search API请求，跳过")
                        continue
                        
                    # 获取响应内容
                    response = entry['response']
                    content = response.get('content', {})
                    
                    if content.get('text'):
                        # 处理响应内容
                        try:
                            response_text = self.response_processor.process_response_content({
                                'content': content,
                                'headers': response.get('headers', [])
                            })
                            self.log(f"成功获取响应内容，长度: {len(response_text)}")
                            
                            # 解析JSON
                            json_data = json.loads(response_text)
                            
                            # 使用响应内容的哈希值作为唯一标识
                            content_hash = hash(str(json_data))
                            if content_hash in processed_contents:
                                self.log("响应内容已处理过，跳过")
                                continue
                                
                            processed_contents.add(content_hash)
                            request_count += 1
                            
                            # 注释掉保存原始JSON的代码，但保留以供将来参考
                            # self.file_handler.save_response_json(json_data, request_count, is_initial)
                            # self.log(f"已保存第 {request_count} 个响应")
                            
                            # 如果是初始请求，标记为已完成
                            # if is_initial:
                            #     is_initial = False
                            
                            # 调用统一的视频解析函数
                            video_data_list = self.youtube_parser.extract_videos_from_json(json_data)
                            self.log(f"已解析第 {request_count} 个响应中的视频数据")
                            
                            # 打印解析出的视频数据列表
                            self.log(f"解析出的视频数据列表:")
                            for video_data in video_data_list:
                                self.log(f"视频ID: {video_data.video_id}")
                                self.log(f"标题: {video_data.title}")
                                self.log(f"观看次数: {video_data.view_count}")
                                self.log(f"发布日期: {video_data.published_date}")
                                self.log(f"频道ID: {video_data.channel_id}")
                                self.log(f"频道名称: {video_data.channel_name}")
                                self.log(f"规范URL: {video_data.canonical_base_url}")
                                self.log("-" * 50)
                            
                        except Exception as e:
                            self.log(f"处理响应时出错: {str(e)}", 'ERROR')
                            continue
                    else:
                        self.log("响应内容为空")
            
            self.log(f"数据分析完成，共处理 {request_count} 个请求")
            return True
            
        except Exception as e:
            self.log(f"处理Shorts内容时出错: {str(e)}", 'ERROR')
            return False

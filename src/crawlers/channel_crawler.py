from browsermobproxy import Server
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
import json
from src.utils import Utils
from src.services import ChannelService
from log_manager import LogManager
import configparser
import random
from datetime import datetime
import base64
import brotli
import os

class ChannelCrawler:
    def __init__(self, worker_id=None, proxy_path=r"C:\Program Files\browsermob-proxy-2.1.4\bin\browsermob-proxy.bat"):
        """初始化频道爬虫"""
        self.proxy_path = proxy_path
        self.worker_id = worker_id
        self.server = None
        self.proxy = None
        self.driver = None
        self.logger = LogManager().get_logger('ChannelCrawler')
        
    def log(self, message, level='INFO'):
        """输出日志"""
        LogManager.log(level, message)
        
    def setup(self):
        """初始化爬虫"""
        try:
            # 读取配置文件
            config = configparser.ConfigParser()
            config.read('config.ini', encoding='utf-8')
            proxy_path = config['proxy']['path']
            
            # 启动代理服务器
            self.log("启动代理服务器...")
            self.proxy_port = 8090 + (self.worker_id * 10 if self.worker_id is not None else 0)
            self.log(f"使用服务器端口: {self.proxy_port}")
            
            self.proxy_server = Server(
                path=proxy_path,
                options={
                    'port': self.proxy_port,
                    'log_path': 'logs/proxy.log',
                    'capture_content': True,  # 确保捕获响应内容
                    'capture_headers': True   # 捕获请求和响应头
                }
            )
            self.proxy_server.start()
            self.log("代理服务器启动成功")
            
            # 创建代理客户端
            self.proxy = self.proxy_server.create_proxy(
                params={
                    'trustAllServers': True,
                    'captureContent': True,   # 确保捕获响应内容
                    'captureHeaders': True    # 捕获请求和响应头
                }
            )
            
            proxy_url = f"localhost:{self.proxy_port + 1}"
            self.log(f"创建代理成功，地址: {proxy_url}")
            
            # 初始化Chrome浏览器
            self.log("初始化Chrome浏览器...")
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_argument(f'--proxy-server={proxy_url}')
            chrome_options.add_argument('--ignore-certificate-errors')
            
            # 设置浏览器语言为英文
            chrome_options.add_argument('--lang=en-US')
            chrome_options.add_experimental_option('prefs', {'intl.accept_languages': 'en-US,en'})
            
            # 禁用图片和视频加载以提高性能
            chrome_prefs = {
                "profile.default_content_setting_values": {
                    "images": 2,       # 1表示启用图片
                    "media_stream": 2, # 禁用媒体流
                    "plugins": 2,      # 禁用插件
                    "video": 2,        # 禁用视频
                    "sound": 2         # 禁用声音
                },
                "profile.managed_default_content_settings": {
                    "images": 2,
                    "media_stream": 2,
                    "sound": 2         # 禁用声音
                },
                "intl.accept_languages": "en-US,en"
            }
            chrome_options.add_experimental_option("prefs", chrome_prefs)
            
            self.driver = webdriver.Chrome(options=chrome_options)
            self.log("Chrome浏览器初始化成功")
            
        except Exception as e:
            self.log(f"初始化爬虫失败: {str(e)}")
            raise
            
    def cleanup(self):
        """清理资源"""
        try:
            if self.driver:
                self.log("正在关闭浏览器...")
                self.driver.quit()
                self.log("浏览器已关闭")
                
            if self.server:
                self.log("正在停止代理服务器...")
                self.server.stop()
                self.log("代理服务器已停止")
                
        except Exception as e:
            self.log(f"清理资源时出: {str(e)}", 'ERROR')
            
    def crawl_channel(self, url):
        """爬取频道信息"""
        try:
            self.log(f"开始爬取频道: {url}")
            max_retries = 3
            retry_count = 0
            
            # 创建responses目录（如果不存在）
            responses_dir = "responses"
            if not os.path.exists(responses_dir):
                os.makedirs(responses_dir)
                self.log(f"创建目录: {responses_dir}")

            while retry_count < max_retries:
                try:
                    # 清除之前的HAR记录
                    self.proxy.new_har(
                        f"channel_{int(time.time())}", 
                        options={
                            'captureHeaders': True,
                            'captureContent': True,
                            'captureBinaryContent': True,
                            'captureEncoding': True
                        }
                    )
                    
                    # 访问频道页面
                    self.driver.get(url)
                    time.sleep(random.uniform(5, 10))
                    
                    # 获取页面上的channel_name
                    page_channel_name = None
                    try:
                        # 使用更简单的选择器
                        selectors_to_try = [
                            "//*[@id='channel-name']",
                            "//h1[contains(@class, 'title')]",
                            "//h1//span",
                            "//*[@id='page-header']//h1//span"
                        ]
                        
                        for selector in selectors_to_try:
                            try:
                                channel_name_element = WebDriverWait(self.driver, 5).until(
                                    EC.presence_of_element_located((By.XPATH, selector))
                                )
                                page_channel_name = channel_name_element.text
                                if page_channel_name:
                                    self.log(f"从选择器 {selector} 获取到channel_name: {page_channel_name}")
                                    break
                            except Exception as selector_error:
                                self.log(f"选择器 {selector} 未找到元素: {str(selector_error)}")
                                continue
                        
                        # 如果所有选择器都失败，保存页面源码和截图以便调试
                        if not page_channel_name:
                            self.log("所有选择器都失败，保存页面源码和截图以便调试")
                            timestamp = time.strftime("%Y%m%d_%H%M%S")
                            
                            # 保存页面源码
                            debug_dir = "debug"
                            if not os.path.exists(debug_dir):
                                os.makedirs(debug_dir)
                                
                            source_path = os.path.join(debug_dir, f"page_source_{timestamp}.html")
                            with open(source_path, "w", encoding="utf-8") as f:
                                f.write(self.driver.page_source)
                            self.log(f"已保存页面源码到: {source_path}")
                            
                            # 保存截图
                            screenshot_path = os.path.join(debug_dir, f"screenshot_{timestamp}.png")
                            self.driver.save_screenshot(screenshot_path)
                            self.log(f"已保存页面截图到: {screenshot_path}")
                            
                            # 频道不存在，返回None
                            self.log("频道不存在，终止处理")
                            return None
                    except Exception as e:
                        self.log(f"获取页面channel_name失败: {str(e)}")
                        self.log("频道不存在，终止处理")
                        return None
                    
                    # 获取频道头像URL
                    avatar_url = None
                    try:
                        avatar_xpath = "//*[@id=\"page-header\"]/yt-page-header-renderer/yt-page-header-view-model/div/div[1]/yt-decorated-avatar-view-model/yt-avatar-shape/div/div/div/img"
                        avatar_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, avatar_xpath))
                        )
                        avatar_url = avatar_element.get_attribute('src')
                        self.log(f"从页面获取到avatar_url: {avatar_url}")
                    except Exception as e:
                        self.log(f"获取频道头像URL失败: {str(e)}")
                        avatar_url = None
                    
                    # 点击"显示更多"区域
                    try:
                        show_more_xpath = """//*[@id="page-header"]/yt-page-header-renderer/yt-page-header-view-model/div/div[1]/div/yt-description-preview-view-model/truncated-text/truncated-text-content/button/span/span"""
                        
                        # 直接尝试定位元素
                        show_more_element = WebDriverWait(self.driver, 5).until(
                            EC.presence_of_element_located((By.XPATH, show_more_xpath))
                        )
                        
                        if show_more_element:
                            # 确保元素在视图中
                            self.driver.execute_script("arguments[0].scrollIntoView(true);", show_more_element)
                            time.sleep(1)
                            
                            # 点击元素
                            self.driver.execute_script("arguments[0].click();", show_more_element)
                            self.log("已点击'显示更多'区域")
                            time.sleep(random.uniform(2, 3))
                        else:
                            self.log("未找到'显示更多'区域")
                            retry_count += 1
                            continue
                        
                    except TimeoutException:
                        self.log("点击'显示更多'区域超时")
                        retry_count += 1
                        continue
                    
                    # 等待并获取API响应
                    time.sleep(2)  # 等待API响应完成
                    
                    # 定义需要捕获的请求URL模式
                    target_url = 'youtubei/v1/browse'
                    processed_entries = set()
                    api_response = None
                    
                    # 处理网络请求
                    for entry in self.proxy.har['log']['entries']:
                        request_url = entry['request']['url']
                        entry_id = f"{request_url}_{entry['startedDateTime']}"
                        
                        if entry_id not in processed_entries and target_url in request_url:
                            self.log(f"\n=== API Request ===")
                            self.log(f"URL: {request_url}")
                            self.log(f"Method: {entry['request']['method']}")
                            self.log(f"Time: {time.strftime('%H:%M:%S')}")
                            
                            # 获取响应内容
                            response = entry['response']
                            if response['content'].get('text'):
                                try:
                                    # 使用Utils处理响应内容
                                    response_text = Utils.process_response_content(response)
                                    self.log(f"响应内容大小: {len(response_text)}")
                                    
                                    response_json = json.loads(response_text)
                                    self.log(f"响应JSON keys: {list(response_json.keys())}")
                                    
                                    # 保存响应JSON
                                    # timestamp = time.strftime("%Y%m%d_%H%M%S")
                                    # filename = os.path.join(
                                    #     responses_dir, 
                                    #     f"response_json_{timestamp}_{self.worker_id}.json"
                                    # )
                                    # with open(filename, 'w', encoding='utf-8') as f:
                                    #     json.dump(response_json, f, ensure_ascii=False, indent=2)
                                    # self.log(f"已保存响应JSON到文件: {filename}")
                                    
                                    # 直接使用响应
                                    api_response = response_json
                                    break
                                        
                                except json.JSONDecodeError as e:
                                    self.log(f"JSON解析错误: {str(e)}")
                                except Exception as e:
                                    self.log(f"处理API响应时出错: {str(e)}")
                            else:
                                self.log("响应内容为空")
                                
                            processed_entries.add(entry_id)
                    
                    if api_response:
                        self.log("成功获取API响应")
                        
                        # 解析频道信息
                        channel_info = Utils.analyze_channel_json_response(response_json, page_channel_name)
                        if channel_info:
                            # 添加头像URL到频道信息中
                            if avatar_url:
                                channel_info['avatar_url'] = avatar_url
                                self.log("已将avatar_url添加到频道信息中")
                            
                            self.log("成功解析频道信息")
                            return channel_info
                        else:
                            self.log("解析频道信息失败")
                    else:
                        self.log("未找到有效的API响应")
                    
                    retry_count += 1
                    time.sleep(random.uniform(2, 5))
                    continue
                    
                except Exception as e:
                    self.log(f"处理频道时出错: {str(e)}")
                    retry_count += 1
                    time.sleep(random.uniform(5, 10))
                    continue
                    
            self.log(f"达到最大重试次数({max_retries})，放弃处理")
            return None
            
        except Exception as e:
            self.log(f"爬取频道时出错: {str(e)}")
            return None

if __name__ == "__main__":
    try:
        # 创建爬虫实例
        crawler = ChannelCrawler()
        
        # 初始化
        crawler.setup()
        
        # 爬取频道
        crawler.crawl_channels()
        
    except Exception as e:
        print(f"程序执行出错: {str(e)}")
    finally:
        # 清理资源
        crawler.cleanup() 
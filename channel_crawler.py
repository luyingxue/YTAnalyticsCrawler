from browsermobproxy import Server
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
import json
from utils import Utils
from db_manager import DBManager
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
                        channel_name_xpath = "/html/body/ytd-app/div[1]/ytd-page-manager/ytd-browse/div[3]/ytd-tabbed-page-header/tp-yt-app-header-layout/div/tp-yt-app-header/div[2]/div/div[2]/yt-page-header-renderer/yt-page-header-view-model/div/div[1]/div/yt-dynamic-text-view-model/h1/span"
                        channel_name_element = WebDriverWait(self.driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, channel_name_xpath))
                        )
                        page_channel_name = channel_name_element.text
                        self.log(f"从页面获取到channel_name: {page_channel_name}")
                    except Exception as e:
                        self.log(f"获取页面channel_name失败: {str(e)}")
                        page_channel_name = None
                    
                    # 点击"显示更多"区域
                    try:
                        # 定义可能的XPATH列表
                        show_more_xpaths = [
                            """/html/body/ytd-app/div[1]/ytd-page-manager/ytd-browse/div[3]/ytd-tabbed-page-header/tp-yt-app-header-layout/div/tp-yt-app-header/div[2]/div/div[2]/yt-page-header-renderer/yt-page-header-view-model/div/div[1]/div/yt-description-preview-view-model/truncated-text""",
                            """/html/body/ytd-app/div[1]/ytd-page-manager/ytd-browse/div[3]/ytd-tabbed-page-header/tp-yt-app-header-layout/div/tp-yt-app-header/div[2]/div/div/yt-page-header-renderer/yt-page-header-view-model/div/div[1]/div/yt-description-preview-view-model/truncated-text"""
                        ]
                        
                        # 尝试每个XPATH
                        show_more_element = None
                        for xpath in show_more_xpaths:
                            try:
                                show_more_element = WebDriverWait(self.driver, 5).until(
                                    EC.presence_of_element_located((By.XPATH, xpath))
                                )
                                if show_more_element:
                                    break
                            except TimeoutException:
                                continue
                        
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
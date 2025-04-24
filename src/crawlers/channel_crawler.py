from browsermobproxy import Server
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import time
import json
from src.utils import ResponseProcessor, YouTubeParser, SelectorUtils
from src.services import ChannelService
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
        from src.utils import Logger
        self.logger = Logger()
        self.response_processor = ResponseProcessor()
        self.youtube_parser = YouTubeParser()
        self.selector_utils = SelectorUtils()
        
    def log(self, message, level='INFO'):
        """输出日志"""
        self.logger.log(message, level, self.worker_id)
        
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
            
            # 添加新的配置参数来解决TensorFlow相关问题
            chrome_options.add_argument('--disable-gpu')  # 禁用GPU硬件加速
            chrome_options.add_argument('--disable-software-rasterizer')  # 禁用软件光栅化
            chrome_options.add_argument('--disable-dev-shm-usage')  # 禁用/dev/shm使用
            chrome_options.add_argument('--no-sandbox')  # 禁用沙箱
            chrome_options.add_argument('--disable-features=NetworkService')  # 禁用网络服务
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')  # 禁用显示合成器
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')  # 禁用自动化控制检测
            chrome_options.add_argument('--disable-machine-learning')  # 禁用机器学习功能
            
            # 设置浏览器语言为英文
            chrome_options.add_argument('--lang=en-US')
            chrome_options.add_experimental_option('prefs', {
                'intl.accept_languages': 'en-US,en',
                # 更新Chrome首选项
                "profile.default_content_setting_values": {
                    "images": 2,
                    "media_stream": 2,
                    "plugins": 2,
                    "video": 2,
                    "sound": 2,
                    "notifications": 2  # 禁用通知
                },
                "profile.managed_default_content_settings": {
                    "images": 2,
                    "media_stream": 2,
                    "sound": 2,
                    "notifications": 2
                },
                "profile.password_manager_enabled": False,  # 禁用密码管理器
                "credentials_enable_service": False,  # 禁用凭据服务
            })
            
            self.driver = webdriver.Chrome(options=chrome_options)
            # 设置页面加载超时
            self.driver.set_page_load_timeout(30)  # 30秒超时
            # 设置脚本执行超时
            self.driver.set_script_timeout(30)  # 30秒超时
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
            
    def get_text_by_selectors(self, selectors, attribute=None, wait_time=5):
        """
        通过多个选择器尝试获取元素文本或属性值
        
        Args:
            selectors (list): XPath选择器列表
            attribute (str, optional): 要获取的属性名，如果为None则获取文本内容
            wait_time (int): 等待元素出现的最大时间（秒）
            
        Returns:
            str: 获取到的文本或属性值，如果所有选择器都失败则返回None
        """
        for selector in selectors:
            try:
                element = WebDriverWait(self.driver, wait_time).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                value = element.get_attribute(attribute) if attribute else element.text
                if value:
                    self.log(f"从选择器 {selector} 获取到{'属性' if attribute else '文本'}: {value}")
                    return value
            except Exception as selector_error:
                self.log(f"选择器 {selector} 未找到元素: {str(selector_error)}")
                continue
        return None

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
                    
                    # 访问频道页面时添加超时处理
                    self.driver.set_page_load_timeout(30)
                    self.driver.get(url)
                    
                    # 如果页面加载成功，重置超时时间为更长的值
                    self.driver.set_page_load_timeout(120)
                    
                    time.sleep(random.uniform(5, 10))
                    
                    # 获取页面上的channel_name
                    channel_name_selectors = [
                        "//*[@id='channel-name']",
                        "//h1[contains(@class, 'title')]",
                        "//h1//span",
                        "//*[@id='page-header']//h1//span"
                    ]
                    
                    page_channel_name = self.selector_utils.get_text_by_selectors(
                        self.driver, 
                        channel_name_selectors,
                        self.logger
                    )
                    
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
                    
                    # 获取频道头像URL
                    avatar_selectors = [
                        "//*[@id='page-header']//yt-avatar-shape//img",
                        "//yt-decorated-avatar-view-model//img",
                        "//*[contains(@class, 'channel-avatar')]//img",
                        "//*[contains(@class, 'avatar')]//img"
                    ]
                    
                    avatar_url = self.selector_utils.get_text_by_selectors(
                        self.driver,
                        avatar_selectors,
                        self.logger,
                        attribute='src'
                    )
                    
                    # 获取前三个视频的封面
                    video_thumbnail_selectors = [
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[1]//img",
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[2]//img",
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[3]//img"
                    ]
                    
                    video_thumbnails = []
                    for i, selector in enumerate(video_thumbnail_selectors, 1):
                        thumbnail_url = self.selector_utils.get_text_by_selectors(
                            self.driver,
                            [selector],
                            self.logger,
                            attribute='src'
                        )
                        if thumbnail_url:
                            video_thumbnails.append(thumbnail_url)
                            self.log(f"获取到第{i}个视频封面: {thumbnail_url}")
                        else:
                            self.log(f"未能获取到第{i}个视频封面")
                    
                    # 获取前三个视频的标题
                    video_title_selectors = [
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[1]//h3/a/span",
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[2]//h3/a/span",
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[3]//h3/a/span"
                    ]
                    
                    video_titles = []
                    for i, selector in enumerate(video_title_selectors, 1):
                        title = self.selector_utils.get_text_by_selectors(
                            self.driver,
                            [selector],
                            self.logger
                        )
                        if title:
                            video_titles.append(title)
                            self.log(f"获取到第{i}个视频标题: {title}")
                        else:
                            self.log(f"未能获取到第{i}个视频标题")
                    
                    # 获取前三个视频的播放量
                    video_views_selectors = [
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[1]//div/div[1]/span",
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[2]//div/div[1]/span",
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[3]//div/div[1]/span"
                    ]
                    
                    video_views = []
                    for i, selector in enumerate(video_views_selectors, 1):
                        views = self.selector_utils.get_text_by_selectors(
                            self.driver,
                            [selector],
                            self.logger
                        )
                        if views:
                            video_views.append(views)
                            self.log(f"获取到第{i}个视频播放量: {views}")
                        else:
                            self.log(f"未能获取到第{i}个视频播放量")
                    
                    # 获取前三个视频的URL
                    video_url_selectors = [
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[1]//ytm-shorts-lockup-view-model/a",
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[2]//ytm-shorts-lockup-view-model/a",
                        "//ytd-rich-grid-renderer//ytd-rich-item-renderer[3]//ytm-shorts-lockup-view-model/a"
                    ]
                    
                    video_urls = []
                    for i, selector in enumerate(video_url_selectors, 1):
                        url = self.selector_utils.get_text_by_selectors(
                            self.driver,
                            [selector],
                            self.logger,
                            attribute='href'
                        )
                        if url:
                            # 处理shorts URL
                            if url.startswith('/shorts/'):
                                video_id = url.split('/shorts/')[1]
                                url = f"https://www.youtube.com/shorts/{video_id}"
                            video_urls.append(url)
                            self.log(f"获取到第{i}个视频URL: {url}")
                        else:
                            self.log(f"未能获取到第{i}个视频URL")
                    
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
                                    # 使用ResponseProcessor处理响应内容
                                    response_text = self.response_processor.process_response_content(response)
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
                        channel_info = self.youtube_parser.analyze_channel_json_response(api_response, page_channel_name)
                        if channel_info:
                            # 添加头像URL到频道信息中
                            if avatar_url:
                                channel_info['avatar_url'] = avatar_url
                                self.log("已将avatar_url添加到频道信息中")
                            
                            # 打包最新视频信息
                            new_videos_info = []
                            for i in range(len(video_thumbnails)):
                                video_info = {
                                    'thumbnail_url': video_thumbnails[i] if i < len(video_thumbnails) else None,
                                    'title': video_titles[i] if i < len(video_titles) else None,
                                    'views': video_views[i] if i < len(video_views) else None,
                                    'url': video_urls[i] if i < len(video_urls) else None
                                }
                                new_videos_info.append(video_info)
                            
                            # 添加最新视频信息到频道信息中
                            channel_info['new_videos_info'] = new_videos_info
                            self.log("已将最新视频信息添加到频道信息中")
                            
                            self.log("成功解析频道信息")
                            return channel_info
                        else:
                            self.log("解析频道信息失败")
                    else:
                        self.log("未找到有效的API响应")
                    
                    retry_count += 1
                    time.sleep(random.uniform(2, 5))
                    continue
                    
                except TimeoutException:
                    self.log("页面加载超时，正在重试...")
                    # 尝试关闭当前标签页
                    try:
                        self.driver.execute_script("window.stop();")
                    except:
                        pass
                    retry_count += 1
                    time.sleep(random.uniform(5, 10))
                    continue
                except WebDriverException as e:
                    self.log(f"WebDriver错误: {str(e)}")
                    retry_count += 1
                    time.sleep(random.uniform(5, 10))
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
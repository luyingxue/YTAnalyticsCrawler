from browsermobproxy import Server
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import json
from src.utils import ResponseProcessor, FileHandler
from src.services import VideoService, ChannelService
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
            
            # 基础稳定性选项
            chrome_options.add_argument('--no-sandbox')
            chrome_options.add_argument('--disable-dev-shm-usage')
            chrome_options.add_argument('--disable-gpu')
            chrome_options.add_argument('--disable-extensions')
            chrome_options.add_argument('--disable-infobars')
            chrome_options.add_argument('--disable-notifications')
            chrome_options.add_argument('--disable-popup-blocking')
            chrome_options.add_argument('--disable-web-security')
            chrome_options.add_argument('--ignore-certificate-errors')
            chrome_options.add_argument('--ignore-ssl-errors')
            chrome_options.add_argument('--lang=zh-CN')
            chrome_options.add_argument('--start-maximized')
            
            # 增强稳定性选项
            chrome_options.add_argument('--disable-blink-features=AutomationControlled')
            chrome_options.add_argument('--disable-software-rasterizer')
            chrome_options.add_argument('--disable-background-timer-throttling')
            chrome_options.add_argument('--disable-backgrounding-occluded-windows')
            chrome_options.add_argument('--disable-renderer-backgrounding')
            chrome_options.add_argument('--disable-features=TranslateUI')
            chrome_options.add_argument('--disable-default-apps')
            chrome_options.add_argument('--disable-sync')
            chrome_options.add_argument('--no-first-run')
            chrome_options.add_argument('--disable-features=VizDisplayCompositor')
            chrome_options.add_argument('--disable-ipc-flooding-protection')
            chrome_options.add_argument('--disable-hang-monitor')
            chrome_options.add_argument('--disable-prompt-on-repost')
            chrome_options.add_argument('--disable-client-side-phishing-detection')
            chrome_options.add_argument('--disable-component-update')
            chrome_options.add_argument('--disable-domain-reliability')
            chrome_options.add_argument('--enable-unsafe-swiftshader')
            
            # 禁用可能导致问题的功能
            chrome_options.add_argument('--disable-machine-learning')
            chrome_options.add_argument('--disable-features=NetworkService')
            chrome_options.add_argument('--disable-features=MediaRouter')
            chrome_options.add_argument('--disable-features=Translate')
            
            # 内存和性能优化
            chrome_options.add_argument('--memory-pressure-off')
            chrome_options.add_argument('--max_old_space_size=4096')
            chrome_options.add_argument('--aggressive-cache-discard')
            
            # 用户代理
            chrome_options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
            
            # 实验性选项设置
            chrome_options.add_experimental_option('excludeSwitches', ['enable-automation'])
            chrome_options.add_experimental_option('useAutomationExtension', False)
            chrome_options.add_experimental_option('prefs', {
                "profile.default_content_setting_values": {
                    "images": 2,  # 禁用图片加载
                    "media_stream": 2,
                    "plugins": 2,
                    "video": 2,
                    "sound": 2,
                    "notifications": 2
                },
                "profile.managed_default_content_settings": {
                    "images": 2,
                    "media_stream": 2,
                    "sound": 2,
                    "notifications": 2
                },
                "profile.password_manager_enabled": False,
                "credentials_enable_service": False,
                "profile.default_content_settings.popups": 0
            })
            
            # 启动Chrome浏览器
            self.driver = webdriver.Chrome(options=chrome_options)
            
            # 设置超时时间
            self.driver.set_page_load_timeout(60)  # 增加页面加载超时时间
            self.driver.set_script_timeout(60)
            self.driver.implicitly_wait(10)
            
            # 执行反检测脚本
            self.driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            
            self.log("爬虫环境设置完成")
            return True
            
        except Exception as e:
            self.log(f"设置爬虫环境时出错: {str(e)}", 'ERROR')
            self.cleanup()
            return False
            
    def cleanup(self):
        """清理爬虫资源"""
        self.log("开始清理爬虫资源...")
        
        # 清理driver
        if self.driver:
            try:
                self.log("正在关闭浏览器...")
                # 先尝试优雅关闭
                try:
                    self.driver.execute_script("window.stop();")
                except:
                    pass
                
                # 关闭所有窗口
                try:
                    for handle in self.driver.window_handles:
                        self.driver.switch_to.window(handle)
                        self.driver.close()
                except:
                    pass
                
                # 退出driver
                try:
                    self.driver.quit()
                except Exception as e:
                    self.log(f"driver.quit()失败: {str(e)}", 'WARNING')
                    
                self.driver = None
                self.log("浏览器已关闭")
                
            except Exception as e:
                self.log(f"清理浏览器时出错: {str(e)}", 'ERROR')
                # 强制清理
                try:
                    import psutil
                    import os
                    # 查找并终止Chrome进程
                    for proc in psutil.process_iter(['pid', 'name']):
                        if 'chrome' in proc.info['name'].lower():
                            try:
                                proc.terminate()
                                proc.wait(timeout=3)
                            except:
                                pass
                except ImportError:
                    self.log("psutil未安装，无法强制清理Chrome进程", 'WARNING')
                except Exception as cleanup_error:
                    self.log(f"强制清理Chrome进程失败: {str(cleanup_error)}", 'WARNING')
                
                self.driver = None
        
        # 清理proxy
        if self.proxy:
            try:
                self.log("正在关闭代理...")
                self.proxy.close()
                self.proxy = None
                self.log("代理已关闭")
            except Exception as e:
                self.log(f"清理代理时出错: {str(e)}", 'WARNING')
                self.proxy = None
        
        # 清理server
        if self.server:
            try:
                self.log("正在停止代理服务器...")
                self.server.stop()
                self.server = None
                self.log("代理服务器已停止")
            except Exception as e:
                self.log(f"清理代理服务器时出错: {str(e)}", 'WARNING')
                self.server = None
        
        self.log("爬虫资源清理完成")
            
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
        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                # 等待页面完全加载
                time.sleep(5)
                
                # 检查页面是否正常加载
                if "youtube.com" not in self.driver.current_url:
                    self.log("页面未正常加载，重试中...", 'WARNING')
                    retry_count += 1
                    continue
                
                # 等待Shorts按钮出现，使用更宽松的选择器
                self.log("正在查找Shorts按钮...")
                
                # 首先尝试打印页面上所有可能的chip按钮，用于调试
                try:
                    self.log("调试: 查找页面上的所有chip按钮...")
                    all_chips = self.driver.find_elements(By.XPATH, "//yt-chip-cloud-chip-renderer//button")
                    self.log(f"调试: 找到 {len(all_chips)} 个chip按钮")
                    
                    for i, chip in enumerate(all_chips[:10]):  # 只显示前10个
                        try:
                            text = chip.text.strip()
                            if text:
                                self.log(f"调试: Chip {i+1}: '{text}'")
                        except:
                            pass
                except Exception as debug_error:
                    self.log(f"调试失败: {str(debug_error)}")
                
                # 尝试多个可能的Shorts按钮选择器
                shorts_selectors = [
                    # 基于真实结构的更精确选择器
                    "//yt-chip-cloud-renderer//iron-selector//yt-chip-cloud-chip-renderer//chip-shape//button[contains(text(), 'Shorts')]",
                    "//yt-chip-cloud-renderer//iron-selector//yt-chip-cloud-chip-renderer[2]//chip-shape//button",
                    "//iron-selector//yt-chip-cloud-chip-renderer//chip-shape//button[contains(text(), 'Shorts')]",
                    
                    # 支持不同语言的选择器
                    "//chip-shape//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'shorts')]",
                    "//yt-chip-cloud-chip-renderer//button[contains(translate(text(), 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'shorts')]",
                    
                    # 备用选择器 - 更宽泛的匹配
                    "//chip-shape//button[contains(text(), 'Shorts')]",
                    "//yt-chip-cloud-chip-renderer//button[contains(text(), 'Shorts')]",
                    "//button[contains(text(), 'Shorts') and contains(@class, 'chip')]",
                    
                    # 通过位置查找（第2个chip通常是Shorts）
                    "//yt-chip-cloud-renderer//iron-selector//yt-chip-cloud-chip-renderer[2]//button",
                    "(//yt-chip-cloud-chip-renderer//button)[2]",
                    
                    # 原有的选择器作为fallback
                    "//yt-chip-cloud-chip-renderer[.//yt-formatted-string[contains(text(), 'Shorts')]]",
                    "//yt-chip-cloud-chip-renderer[.//span[contains(text(), 'Shorts')]]",
                    "//button[contains(text(), 'Shorts')]",
                    "//*[contains(@aria-label, 'Shorts')]",
                    "//*[contains(text(), 'Shorts')]"
                ]
                
                shorts_button = None
                for selector in shorts_selectors:
                    try:
                        shorts_button = WebDriverWait(self.driver, 10).until(
                            EC.element_to_be_clickable((By.XPATH, selector))
                        )
                        self.log(f"找到Shorts按钮，使用选择器: {selector}")
                        break
                    except Exception as e:
                        self.log(f"选择器 {selector} 未找到Shorts按钮: {str(e)}")
                        continue
                
                if not shorts_button:
                    self.log("未找到Shorts按钮，尝试下一次重试", 'WARNING')
                    retry_count += 1
                    time.sleep(3)
                    continue
                
                # 开始监视网络请求
                self.log("开始监视网络请求")
                try:
                    self.proxy.new_har("youtube", options={
                        'captureHeaders': True,
                        'captureContent': True,
                        'captureBinaryContent': True,
                        'captureEncoding': True,
                        'urlPattern': '.*/youtubei/v1/search.*'  # 捕获所有youtubei/v1/search请求
                    })
                except Exception as proxy_error:
                    self.log(f"代理设置失败: {str(proxy_error)}", 'WARNING')
                
                # 滚动到按钮位置并点击
                try:
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", shorts_button)
                    time.sleep(1)
                    
                    # 尝试多种点击方式
                    click_success = False
                    
                    # 方式1：普通点击
                    try:
                        shorts_button.click()
                        click_success = True
                        self.log("使用普通点击成功")
                    except Exception as e:
                        self.log(f"普通点击失败: {str(e)}")
                    
                    # 方式2：JavaScript点击
                    if not click_success:
                        try:
                            self.driver.execute_script("arguments[0].click();", shorts_button)
                            click_success = True
                            self.log("使用JavaScript点击成功")
                        except Exception as e:
                            self.log(f"JavaScript点击失败: {str(e)}")
                    
                    if not click_success:
                        self.log("所有点击方式都失败，重试中...", 'WARNING')
                        retry_count += 1
                        continue
                    
                    self.log("已点击Shorts按钮")
                    time.sleep(5)  # 等待页面响应
                    
                except Exception as click_error:
                    self.log(f"点击Shorts按钮时出错: {str(click_error)}", 'ERROR')
                    retry_count += 1
                    continue
                
                # 初始化变量
                scroll_count = 0
                max_scrolls = 10
                request_count = 0
                is_initial = True
                processed_contents = set()  # 用于跟踪已处理的响应内容
                all_channel_ids = set()  # 用于存储所有唯一的channel_id
                
                # 执行滚动操作
                self.log("开始执行页面滚动")
                while scroll_count < max_scrolls:
                    try:
                        # 使用更可靠的滚动方式
                        self.driver.execute_script("window.scrollTo(0, document.documentElement.scrollHeight);")
                        time.sleep(3)  # 增加等待时间，确保页面加载完成
                        
                        # 增加滚动计数
                        scroll_count += 1
                        self.log(f"已完成第 {scroll_count} 次滚动")
                        
                        # 检查driver是否还有效
                        try:
                            current_url = self.driver.current_url
                            if not current_url:
                                self.log("浏览器连接已断开", 'ERROR')
                                break
                        except Exception as e:
                            self.log(f"浏览器连接检查失败: {str(e)}", 'ERROR')
                            break
                        
                        # 获取当前的HAR日志
                        try:
                            entries = self.proxy.har['log']['entries']
                            self.log(f"当前捕获到 {len(entries)} 个请求")
                            
                            # 只处理新的search请求
                            for entry in entries:
                                request_url = entry['request']['url']
                                
                                # 检查是否是YouTube search API请求
                                if '/youtubei/v1/search' not in request_url:
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
                                            continue
                                            
                                        processed_contents.add(content_hash)
                                        request_count += 1
                                        
                                        # 调用统一的视频解析函数
                                        video_data_list = self.youtube_parser.extract_videos_from_json(json_data)
                                        self.log(f"已解析第 {request_count} 个响应中的视频数据")
                                        
                                        # 收集channel_id
                                        for video_data in video_data_list:
                                            if video_data.channel_id:
                                                all_channel_ids.add(video_data.channel_id)
                                        
                                    except Exception as e:
                                        self.log(f"处理响应时出错: {str(e)}", 'ERROR')
                                        continue
                        except Exception as har_error:
                            self.log(f"获取HAR日志时出错: {str(har_error)}", 'WARNING')
                    
                    except Exception as scroll_error:
                        self.log(f"滚动操作出错: {str(scroll_error)}", 'ERROR')
                        break
                
                # 打印所有收集到的唯一channel_id
                self.log(f"数据分析完成，共处理 {request_count} 个请求")
                self.log(f"共收集到 {len(all_channel_ids)} 个唯一频道ID:")
                for channel_id in all_channel_ids:
                    self.log(f"频道ID: {channel_id}")
                    
                # 调用批量插入频道函数
                if all_channel_ids:
                    try:
                        channel_service = ChannelService()
                        channel_service.batch_add_channels(list(all_channel_ids))
                        self.log("频道数据批量插入成功")
                    except Exception as db_error:
                        self.log(f"频道数据插入失败: {str(db_error)}", 'ERROR')
                
                return True
                
            except Exception as e:
                self.log(f"处理Shorts内容时出错 (重试 {retry_count + 1}/{max_retries}): {str(e)}", 'ERROR')
                retry_count += 1
                
                if retry_count < max_retries:
                    self.log(f"等待 {5 * retry_count} 秒后重试...", 'INFO')
                    time.sleep(5 * retry_count)
                else:
                    self.log("达到最大重试次数，放弃处理", 'ERROR')
                    break
        
        return False

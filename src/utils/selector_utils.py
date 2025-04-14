from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By

class SelectorUtils:
    @staticmethod
    def get_text_by_selectors(driver, selectors, logger=None, attribute=None, wait_time=5):
        """
        通过多个选择器尝试获取元素文本或属性值
        
        Args:
            driver: WebDriver实例
            selectors (list): XPath选择器列表
            logger: 日志记录器实例
            attribute (str, optional): 要获取的属性名，如果为None则获取文本内容
            wait_time (int): 等待元素出现的最大时间（秒）
            
        Returns:
            str: 获取到的文本或属性值，如果所有选择器都失败则返回None
        """
        def log(message, level='INFO'):
            if logger:
                logger.log(message, level)
            else:
                print(f"[{level}] {message}")
                
        for selector in selectors:
            try:
                element = WebDriverWait(driver, wait_time).until(
                    EC.presence_of_element_located((By.XPATH, selector))
                )
                value = element.get_attribute(attribute) if attribute else element.text
                if value:
                    log(f"从选择器 {selector} 获取到{'属性' if attribute else '文本'}: {value}")
                    return value
            except Exception as selector_error:
                log(f"选择器 {selector} 未找到元素: {str(selector_error)}")
                continue
        return None 
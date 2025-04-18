import json
from datetime import datetime
from src.services.keyword_service import KeywordService

def test_get_uncrawled_keywords():
    """测试获取未爬取关键词的功能"""
    try:
        # 创建服务实例
        keyword_service = KeywordService()
        
        print(f"开始测试获取未爬取关键词 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 获取未爬取的关键词
        result = keyword_service.get_uncrawled_keywords()
        
        # 打印结果
        if result:
            print("\n成功获取到关键词信息:")
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
            
            # 打印关键信息
            print("\n关键信息:")
            print(f"关键词ID: {result.get('id')}")
            print(f"关键词: {result.get('key_words')}")
        else:
            print("\n没有找到未爬取的关键词")
            
    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback
        print(f"错误详情:\n{traceback.format_exc()}")

if __name__ == "__main__":
    test_get_uncrawled_keywords() 
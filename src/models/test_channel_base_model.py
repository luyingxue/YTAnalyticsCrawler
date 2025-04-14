import os
import json
from datetime import datetime
from .channel_base_model import ChannelBaseModel

def test_rpc_call():
    """测试RPC调用函数"""
    try:
        # 创建ChannelBaseModel实例
        channel_model = ChannelBaseModel()
        
        # 调用RPC函数
        print("正在调用get_next_uncrawled_channel存储过程...")
        result = channel_model.call_rpc('get_next_uncrawled_channel')
        
        # 打印结果
        if result:
            print("\n成功获取到频道信息:")
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
            
            # 打印关键信息
            print("\n关键信息:")
            print(f"频道ID: {result.get('channel_id')}")
            print(f"频道名称: {result.get('channel_name')}")
            print(f"是否基准频道: {result.get('is_benchmark')}")
            print(f"最后爬取日期: {result.get('last_crawl_date')}")
        else:
            print("\n没有找到未爬取的频道")
            
    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback
        print(f"错误详情:\n{traceback.format_exc()}")

if __name__ == "__main__":
    print(f"开始测试 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    test_rpc_call()
    print(f"测试完成 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}") 
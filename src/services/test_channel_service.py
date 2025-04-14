import json
from datetime import datetime
from src.services.channel_service import ChannelService

def test_get_uncrawled_channel():
    """测试获取未爬取频道的功能"""
    try:
        # 创建服务实例
        channel_service = ChannelService()
        
        print(f"开始测试获取未爬取频道 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 获取未爬取的频道
        result = channel_service.get_uncrawled_channel()
        
        # 打印结果
        if result:
            print("\n成功获取到频道信息:")
            print(json.dumps(result, indent=2, ensure_ascii=False, default=str))
            
            # 打印关键信息
            print("\n关键信息:")
            print(f"频道ID: {result.get('channel_id')}")
            print(f"频道URL: {result.get('url')}")
            print(f"是否基准频道: {result.get('is_benchmark')}")
            print(f"最后爬取日期: {result.get('last_crawl_date')}")
        else:
            print("\n没有找到未爬取的频道")
            
    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback
        print(f"错误详情:\n{traceback.format_exc()}")

def test_insert_channel_crawl():
    """测试插入频道爬取数据的方法"""
    try:
        # 创建服务实例
        channel_service = ChannelService()
        
        print(f"开始测试插入频道爬取数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 模拟频道数据
        channel_info = {
            'channel_id': 'UC_1toTQt6h3Tc1a_F6AE01A',
            'channel_name': '测试频道',
            'description': '这是一个用于测试的YouTube频道',
            'canonical_url': 'https://www.youtube.com/channel/UC_1toTQt6h3Tc1a_F6AE01A',
            'avatar_url': 'https://yt3.googleusercontent.com/test_avatar.jpg',
            'joined_date': '2020-01-01',
            'country': 'CN',
            'subscriber_count': 10000,
            'video_count': 150,
            'view_count': 500000
        }
        
        # 调用插入方法
        result = channel_service.insert_channel_crawl(channel_info)
        
        # 打印结果
        if result:
            print("\n成功插入频道爬取数据")
            print(f"频道ID: {channel_info['channel_id']}")
            print(f"频道名称: {channel_info['channel_name']}")
            print(f"订阅数: {channel_info['subscriber_count']}")
            print(f"视频数: {channel_info['video_count']}")
            print(f"观看数: {channel_info['view_count']}")
        else:
            print("\n插入频道爬取数据失败")
            
    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback
        print(f"错误详情:\n{traceback.format_exc()}")

def test_validate_channel_info():
    """测试频道信息验证方法"""
    try:
        # 创建服务实例
        channel_service = ChannelService()
        
        print(f"开始测试频道信息验证 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 测试数据
        channel_info = {
            'channel_id': 'UC_1toTQt6h3Tc1a_F6AE01A',
            'channel_name': '测试频道',
            'description': '这是一个用于测试的YouTube频道',
            'canonical_url': 'https://www.youtube.com/channel/UC_1toTQt6h3Tc1a_F6AE01A',
            'avatar_url': 'https://yt3.googleusercontent.com/test_avatar.jpg',
            'joined_date': '2020-01-01',
            'country': 'CN',
            'subscriber_count': 10000,
            'video_count': 150,
            'view_count': 500000
        }
        
        # 直接调用_validate_channel_info方法
        result = channel_service._validate_channel_info(channel_info)
        
        # 打印结果
        print(f"\n验证结果: {'通过' if result else '失败'}")
        
        # 测试空数据
        empty_result = channel_service._validate_channel_info({})
        print(f"空数据验证结果: {'通过' if empty_result else '失败'}")
        
        # 测试缺少channel_id的数据
        no_id_result = channel_service._validate_channel_info({'channel_name': '测试频道'})
        print(f"缺少channel_id验证结果: {'通过' if no_id_result else '失败'}")
        
    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback
        print(f"错误详情:\n{traceback.format_exc()}")

def test_delete_channel():
    """测试删除频道功能"""
    try:
        # 创建服务实例
        channel_service = ChannelService()
        
        print(f"开始测试删除频道 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 要删除的频道ID
        channel_id = 'UC_1toTQt6h3Tc1a_F6AE01A'
        
        # 调用删除方法
        result = channel_service.delete_channel(channel_id)
        
        # 打印结果
        if result:
            print(f"\n成功删除频道: {channel_id}")
        else:
            print(f"\n删除频道失败: {channel_id}")
            
    except Exception as e:
        print(f"测试过程中出现错误: {str(e)}")
        import traceback
        print(f"错误详情:\n{traceback.format_exc()}")

if __name__ == "__main__":
    # 测试获取未爬取频道
    # test_get_uncrawled_channel()
    
    # 测试插入频道爬取数据
    # test_insert_channel_crawl()
    
    # 测试频道信息验证
    # test_validate_channel_info()
    
    # 测试删除频道
    test_delete_channel() 
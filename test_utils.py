import json
from utils import Utils

class TestUtils:
    @staticmethod
    def test_analyze_channel_response():
        """测试处理频道响应的JSON数据"""
        print("\n开始测试频道响应数据处理...")
        
        test_files = [
            'channel_response_20241117_150438_1.json',
            'channel_response_20241117_142919_1.json',
            'channel_response_20241117_145816_1.json'
        ]
        
        for file_name in test_files:
            try:
                print(f"\n测试文件: {file_name}")
                with open(file_name, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    
                channel_data = Utils.analyze_channel_json_response(json_data)
                
                if channel_data:
                    print("\n频道信息:")
                    for key, value in channel_data.items():
                        print(f"{key}: {value}")
                else:
                    print("分析失败")
                    
            except FileNotFoundError:
                print(f"未找到文件: {file_name}")
            except json.JSONDecodeError as e:
                print(f"JSON解析错误: {str(e)}")
            except Exception as e:
                print(f"测试过程中出现错误: {str(e)}")

if __name__ == "__main__":
    # 运行测试
    TestUtils.test_analyze_channel_response() 
import json
from utils import Utils

class TestUtils:
    @staticmethod
    def test_analyze_first_response():
        """测试处理第一次响应的JSON数据"""
        print("\n开始测试第一次响应数据处理...")
        
        try:
            # 读取JSON文件
            with open('response_json_20241115_103235_initial_1.json', 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                
            # 调用分析方法
            results = Utils.analyze_and_store_json_response_first(json_data)
            
            # 打印分析结果
            print(f"\n成功处理 {len(results)} 条数据")
            if results:
                print("\n第一条数据示例:")
                for key, value in results[0].items():
                    print(f"{key}: {value}")
                
        except FileNotFoundError:
            print("未找到JSON文件")
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {str(e)}")
        except Exception as e:
            print(f"测试过程中出现错误: {str(e)}")
            
    @staticmethod
    def test_analyze_continuation_response():
        """测试处理后续响应的JSON数据"""
        print("\n开始测试后续响应数据处理...")
        
        try:
            # 读取JSON文件
            with open('response_json_20241115_103235_continuation_2.json', 'r', encoding='utf-8') as f:
                json_data = json.load(f)
                
            # 调用分析方法
            results = Utils.analyze_and_store_json_response_else(json_data)
            
            # 打印分析结果
            print(f"\n成功处理 {len(results)} 条数据")
            if results:
                print("\n第一条数据示例:")
                for key, value in results[0].items():
                    print(f"{key}: {value}")
                
        except FileNotFoundError:
            print("未找到JSON文件")
        except json.JSONDecodeError as e:
            print(f"JSON解析错误: {str(e)}")
        except Exception as e:
            print(f"测试过程中出现错误: {str(e)}")

if __name__ == "__main__":
    # 运行测试
    # TestUtils.test_analyze_first_response()
    TestUtils.test_analyze_continuation_response() 
from utils import Utils
import json

def test_with_demo_files():
    """
    使用演示文件进行测试
    """
    # 在这里设置断点
    # print("\n测试HTML响应...")
    # try:
    #     with open('demo_html.html', 'r', encoding='utf-8') as f:
    #         html_content = f.read()
    #         print("-" * 50)
    #         print(f"HTML长度: {len(html_content)} 字符")
    #         # 在这里设置断点
    #         Utils.analyze_and_store_html_response(html_content)
    # except FileNotFoundError:
    #     print("未找到 demo_html.html 文件")
    
    # 测试JSON响应
    print("\n测试JSON响应...")
    try:
        with open('response_json_20241107_153347_initial_1.json', 'r', encoding='utf-8') as f:
            json_content = f.read()
            try:
                json_data = json.loads(json_content)
                # 在这里设置断点
                # Utils.analyze_and_store_json_response_first(json_data)
                Utils.analyze_and_store_shorts_json_response(json_data)
            except json.JSONDecodeError as e:
                print(f"JSON解析错误: {str(e)}")
    except FileNotFoundError:
        print("未找到 demo_json.json 文件")

if __name__ == "__main__":
    # 在这里设置断点
    print("开始测试...")
    test_with_demo_files() 
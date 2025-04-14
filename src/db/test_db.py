from database import Database
import traceback
import json

def test_database_connection():
    """测试数据库连接"""
    try:
        db = Database()
        print("数据库连接成功！")
        
        # 测试调用存储过程
        result = db.client.rpc('get_next_uncrawled_channel').execute()
        print(f"\n获取下一个未爬取的频道：")
        if result.data:
            print(json.dumps(result.data[0], indent=2, ensure_ascii=False))
        else:
            print("没有找到未爬取的频道")
        
        return db
    except Exception as e:
        print(f"测试过程中出现错误：{str(e)}")
        print(f"错误详情：\n{traceback.format_exc()}")
        return None

if __name__ == "__main__":
    test_database_connection() 
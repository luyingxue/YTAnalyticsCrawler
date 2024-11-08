from db_manager import DBManager

def main():
    try:
        db = DBManager()
        db.test_connection()
    except Exception as e:
        print(f"测试过程出错: {str(e)}")

if __name__ == "__main__":
    main() 
class DBError(Exception):
    """数据库操作基础异常类"""
    pass

class DBConnectionError(DBError):
    """数据库连接异常"""
    pass

class DBQueryError(DBError):
    """数据库查询异常"""
    pass

class DBPoolError(DBError):
    """数据库连接池异常"""
    pass 
# YouTube Analytics Crawler

一个基于Python的YouTube数据爬虫工具，支持多进程并行抓取，数据自动去重和批量处理。

## 项目结构

```
.
├── src/                    # 源代码目录
│   ├── crawlers/          # 爬虫相关代码
│   │   ├── channel_crawler.py  # 频道爬虫
│   │   └── crawler.py     # 基础爬虫类
│   ├── db/                # 数据库相关代码
│   │   ├── pool.py        # 数据库连接池
│   │   └── exceptions.py  # 数据库异常类
│   ├── models/            # 数据模型
│   │   └── base_model.py  # 基础模型类
│   ├── services/          # 业务逻辑服务
│   │   ├── channel_service.py  # 频道服务
│   │   └── video_service.py    # 视频服务
│   └── utils/             # 工具类
│       ├── logger.py      # 日志工具
│       ├── response_processor.py  # 响应处理
│       └── youtube_parser.py     # YouTube解析器
├── logs/                  # 日志目录
├── responses/             # 响应数据目录
├── debug/                 # 调试数据目录
├── config.ini            # 配置文件
├── config.template.ini   # 配置文件模板
├── main.py               # 主程序入口
└── requirements.txt      # 依赖包列表
```

## 主要功能

### 数据抓取
- 支持频道数据抓取
- 支持视频数据抓取
- 自动处理页面滚动加载
- 多进程并行抓取

### 数据处理
- 数据自动清理和格式化
- 批量数据插入
- 自动数据去重
- 事务支持

### 多进程支持
- 自动根据CPU核心数分配进程
- 每个进程独立处理一个任务
- 进程级别的错误处理和重试
- 资源自动清理

### 日志系统
- 按日期自动分割日志文件
- 支持多进程日志记录
- 不同级别的日志（INFO/WARNING/ERROR）
- 详细的操作和错误记录

## 系统要求

### 环境要求
- Python 3.6+
- Chrome浏览器
- BrowserMob Proxy 2.1.4
- Supabase账号和项目

### Python依赖
```bash
pip install -r requirements.txt
```

## 安装配置

### 1. Supabase配置
1. 创建Supabase项目
2. 获取项目URL和service_role key
3. 创建必要的数据表

### 2. 配置文件
复制 `config.template.ini` 为 `config.ini` 并修改：
```ini
[supabase]
url = your_supabase_project_url
key = your_supabase_service_role_key

[crawler]
max_scrolls = 2
scroll_wait_time = 2
page_load_wait = 3
retry_count = 3
retry_wait = 300

[proxy]
path = C:\Program Files\browsermob-proxy-2.1.4\bin\browsermob-proxy.bat

[log]
level = INFO
file_path = logs
retention_days = 7
```

## 使用方法

### 运行爬虫
```bash
python main.py
```

### 查看日志
- 运行日志：`logs/crawler_YYYYMMDD.log`
- 数据库操作日志：`logs/db_operations.log`

## 数据结构

### channel_base表字段说明
- channel_id: 频道唯一标识
- channel_name: 频道名称
- is_benchmark: 是否是对标频道
- is_blacklist: 是否是黑名单频道

### channel_crawl表字段说明
- channel_id: 频道ID
- crawl_date: 抓取日期
- video_count: 视频数量
- subscriber_count: 订阅者数量
- view_count: 总观看次数

### 数据去重机制
- 使用(channel_id, crawl_date)复合唯一索引
- 同一天相同频道只保存一次
- 不同天的相同频道可以保存
- 支持多进程并发抓取

## 注意事项

- 确保Supabase项目已正确配置
- 确保BrowserMob Proxy已正确安装
- 需要稳定的网络连接
- 遵守YouTube的使用条款和政策
- 建议适当控制抓取频率

## 许可证

MIT License

## 作者

WILL.LU

## 贡献

欢迎提交Issue和Pull Request！
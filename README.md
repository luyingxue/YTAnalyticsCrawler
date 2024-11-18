# YouTube Shorts Crawler

一个基于Python的YouTube Shorts视频信息爬虫工具，支持多进程并行抓取，数据自动去重和批量处理。

## 主要功能

### 数据抓取
- 支持搜索页面的Shorts视频抓取
- 支持话题标签(hashtag)页面的Shorts视频抓取
- 自动处理页面滚动加载
- 自动点击Shorts筛选按钮

### 数据处理
- 视频标题自动清理（移除观看次数后缀）
- 观看次数格式化（支持"万"单位转换）
- 批量数据插入
- 自动数据去重（基于video_id和crawl_date）

### 多进程支持
- 自动根据CPU核心数分配进程
- 每个进程独立处理一个URL
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
- MySQL 8.0+
- Chrome浏览器
- BrowserMob Proxy 2.1.4

### Python依赖
bash
pip install -r requirements.txt

## 安装配置

### 1. 数据库配置
sql
-- 创建数据库
CREATE DATABASE youtube_data;
-- 创建用户并授权
CREATE USER 'youtube_crawler'@'%' IDENTIFIED BY 'your_password';
GRANT ALL PRIVILEGES ON youtube_data. TO 'youtube_crawler'@'%';
FLUSH PRIVILEGES;
-- 创建视频表
CREATE TABLE videos (
id BIGINT AUTO_INCREMENT PRIMARY KEY,
video_id VARCHAR(20),
title VARCHAR(255),
view_count INT,
published_date DATE,
crawl_date DATE,
channel_id VARCHAR(30),
channel_name VARCHAR(100),
UNIQUE KEY unique_video_date (video_id, crawl_date)
);
-- 创建URL管理表
CREATE TABLE search_urls (
id BIGINT AUTO_INCREMENT PRIMARY KEY,
url VARCHAR(255) NOT NULL,
description VARCHAR(255),
is_active BOOLEAN DEFAULT TRUE,
last_crawl_time DATETIME,
created_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

### 2. 配置文件
复制config.ini.example为config.ini并修改：
ini
[database]
host = your_mysql_host
database = youtube_data
user = youtube_crawler
password = your_password
[crawler]
max_scrolls = 2 # 每个URL滚动次数
scroll_wait_time = 2 # 滚动等待时间(秒)
page_load_wait = 3 # 页面加载等待时间(秒)
retry_count = 3 # 失败重试次数
retry_wait = 300 # 重试等待时间(秒)
[proxy]
path = C:\Program Files\browsermob-proxy-2.1.4\bin\browsermob-proxy.bat
[log]
level = INFO # 日志级别
file_path = logs # 日志文件路径
retention_days = 7 # 日志保留天数

### 3. 添加抓取URL
sql
INSERT INTO search_urls (url, description) VALUES
('https://www.youtube.com/results?search_query=baby+fashion+show', 'Baby fashion show search'),
('https://www.youtube.com/hashtag/kidsfashion/shorts', 'Kids fashion hashtag');

## 使用方法

### 运行爬虫
bash
python main.py

### 查看日志
- 运行日志：logs/crawler_YYYYMMDD.log
- 数据库操作日志：logs/db_operations.log

## 数据结构

### videos表字段说明
- video_id: 视频唯一标识
- title: 视频标题（已处理，移除观看次数后缀）
- view_count: 观看次数（已转换为整数）
- published_date: 发布日期（普通视频）
- crawl_date: 抓取日期
- channel_id: 频道ID（普通视频）
- channel_name: 频道名称（普通视频）

### 数据去重机制
- 使用(video_id, crawl_date)复合唯一索引
- 同一天相同视频只保存一次
- 不同天的相同视频可以保存
- 支持多进程并发抓取

## 注意事项

- 确保MySQL服务器已启动并可访问
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



## 重要代码备注

### 在数据库中选择当天需要爬取的频道ID
SELECT 
    cb.channel_id,
    cb.is_benchmark,
    MAX(cc.crawl_date) as last_crawl_date
FROM channel_base cb
LEFT JOIN channel_crawl cc ON cb.channel_id = cc.channel_id
WHERE 
    -- 排除今天已爬取的
    cb.channel_id NOT IN (
        SELECT channel_id 
        FROM channel_crawl 
        WHERE crawl_date = CURRENT_DATE
    )
    -- 排除黑名单
    AND cb.is_blacklist = 0
GROUP BY 
    cb.channel_id,
    cb.is_benchmark
ORDER BY 
    -- 对标频道优先
    cb.is_benchmark DESC,
    -- 其次按最后爬取时间排序（NULL值最先）
    CASE 
        WHEN MAX(cc.crawl_date) IS NULL THEN 0 
        ELSE 1 
    END,
    last_crawl_date ASC;
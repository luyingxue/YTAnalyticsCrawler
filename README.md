# YouTube Shorts Crawler

一个用于抓取YouTube Shorts视频信息的爬虫工具。支持通过搜索关键词或话题标签(hashtag)来获取短视频数据。

## 功能特点

- 支持搜索页面的Shorts视频抓取
- 支持话题标签(hashtag)页面的Shorts视频抓取
- 自动处理页面滚动加载
- 数据直接保存到MySQL数据库
- 支持视频基本信息的提取（标题、观看次数等）
- 支持数据去重（同一天相同视频只保存一次）

## 环境要求

- Python 3.6+
- Chrome浏览器
- BrowserMob Proxy 2.1.4
- Selenium WebDriver
- MySQL 8.0+
- 相关Python包（见requirements.txt）

## 安装步骤

1. 克隆仓库：
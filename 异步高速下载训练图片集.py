"""
异步图片下载器 - 从Bing图片搜索下载并处理图片
功能：
1. 多线程异步下载图片
2. 自动调整图片尺寸和格式
3. 过滤小尺寸和重复图片

程序开发：沈松华
最后更新：2024-07-06
版本：1.0.0
GitHub仓库：URL_ADDRESSGitHub仓库：https://github.com/sshuahua/async_image_downloader
"""

import asyncio
import aiohttp
import aiofiles
import os
import sys
import urllib.parse
import urllib3
from urllib.parse import urlparse
from PIL import Image
from bs4 import BeautifulSoup
import logging

class Config:
    """程序运行时配置参数"""
    def __init__(self):
        # 初始化默认配置
        self.keyword = 'orange'      # 图片搜索关键词
        self.target_folder = 'train_data'  # 图片保存目录
        self.min_file_size = 2 * 1024  # 最小文件大小(2KB)
        self.target_width = 200     # 目标图片宽度
        self.target_height = 200    # 目标图片高度
        self.max_pages = 1        # 最大搜索页数
        self.target_format = 'jpg' # 目标图片格式
        
        # 需要跳过的域名(防盗链网站)
        self.skip_domains = [
            "wallpaperflare.com",
            "healthjade.com",
            "specialtyproduce.com"
        ]        
        
        # HTTP请求头设置
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
            'Accept-Language': 'zh-CN,zh;q=0.8,en-US;q=0.5,en;q=0.3'
        }

    def update_from_args(self, args):
        """
        从命令行参数更新配置
        
        Args:
            args: 命令行参数对象
        """
        if args.keyword:
            self.keyword = args.keyword
        if hasattr(args, 'target_folder'):  
            self.target_folder = args.target_folder
        if args.min_file_size:
            self.min_file_size = args.min_file_size * 1024
        if args.target_width:
            self.target_width = args.target_width
        if args.target_height:
            self.target_height = args.target_height
        if hasattr(args, 'max_pages'):
            self.max_pages = args.max_pages

# 禁用 SSL 证书验证警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志记录
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()  # 只保留控制台输出
    ]
)

def setup_environment(config):
    """
    初始化下载环境
    
    Args:
        config: 配置对象
        
    Returns:
        tuple: (保存文件夹路径, 起始索引, 初始URL)
    """
    encoded_keyword = urllib.parse.quote(config.keyword)
    # 使用目标文件夹+关键词作为保存路径
    save_folder = os.path.join(config.target_folder, config.keyword)
    if not os.path.exists(save_folder):
        os.makedirs(save_folder, exist_ok=True)
    
    # 获取当前最大序号
    existing_files = [f for f in os.listdir(save_folder) 
                     if f.startswith(config.keyword) and f.endswith(config.target_format)]  # 改为目标格式过滤
    start_index = len(existing_files) + 1 if existing_files else 1
    
    # 构造搜索字符串
    url = f'https://cn.bing.com/images/async?q={encoded_keyword}&first=0&count=30&cw=1920&ch=937&relp=30&tsc=ImageBasicHover&datsrc=N_I&layout=RowBased&mmasync=1&dgState=x*175_y*848_h*199_c*1_i*106_r*0'
    config.headers['Referer'] = f'https://cn.bing.com/images/search?q={encoded_keyword}'
    return save_folder, start_index, url


def should_skip(url, config):
    """
    检查URL是否应该跳过不下载
    
    Args:
        url: 待检查的图片URL
        config: 配置对象
        
    Returns:
        bool: True表示跳过, False表示可以下载
    """
    try:
        # 跳过base64编码的图片URL，因为通常是小图和占位符
        if url.startswith('data:image/'):
            logging.info(f'跳过base64编码图片: {url[:50]}...')
            return True
            
        parsed_url = urlparse(url)
        domain = parsed_url.netloc
        for skip_domain in config.skip_domains:
            if domain.endswith(skip_domain):
                return True
        return False
    except ValueError:
        logging.error(f"解析 URL {url} 时出错，将跳过该 URL")
        return True

async def download_image(session, img_url, save_folder, start_index, downloaded_count, semaphore, config):
    """
    异步下载单张图片
    
    Args:
        session: aiohttp会话对象
        img_url: 图片URL
        save_folder: 保存目录
        start_index: 起始索引
        downloaded_count: 已下载计数
        semaphore: 信号量控制并发
        config: 配置对象
        
    Returns:
        str: 成功返回文件路径，失败返回None
    """
    async with semaphore:
        if should_skip(img_url, config):
            return None

        # 创建缓存文件夹
        cache_folder = os.path.join(save_folder, '.cache')
        if not os.path.exists(cache_folder):
            os.makedirs(cache_folder, exist_ok=True)

        max_retries = 3
        temp_filename = f"temp_{downloaded_count}.tmp"
        temp_path = os.path.join(cache_folder, temp_filename)
        
        for retry in range(max_retries):
            try:
                async with session.get(img_url, headers=config.headers, timeout=15) as response:
                    if response.status == 200:
                        async with aiofiles.open(temp_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(1024)
                                if not chunk:
                                    break
                                await f.write(chunk)
                        
                        await asyncio.sleep(0.1)
                        
                        file_size = os.path.getsize(temp_path)
                        if file_size >= config.min_file_size:
                            # 直接使用URL中的文件名
                            parsed_url = urlparse(img_url)
                            filename = os.path.basename(parsed_url.path)
                            final_path = os.path.join(save_folder, filename)
                            
                            if os.path.exists(final_path):
                                os.remove(final_path)
                            os.rename(temp_path, final_path)
                            return final_path
                        else:
                            logging.info(f'{img_url} 图片小于最小尺寸要求')
                            if os.path.exists(temp_path):
                                os.remove(temp_path)
                    else:
                        logging.warning(f'下载失败，状态码: {response.status}')
            except Exception as e:
                logging.error(f'下载错误: {e}')
                if retry < max_retries - 1:
                    await asyncio.sleep(2)
            finally:
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except:
                        pass
        return None

def process_image(file_path, config):
    """
    处理图片文件：转换格式和调整尺寸
    
    Args:
        file_path: 图片文件路径
        config: 配置对象
        
    Returns:
        bool: 处理成功返回True，否则返回False
    """
    try:
        # 检查文件大小
        file_size = os.path.getsize(file_path)
        if file_size < config.min_file_size:
            logging.info(f'跳过小尺寸图片: {file_path} (大小: {file_size/1024:.1f}KB < {config.min_file_size/1024:.1f}KB)')
            os.remove(file_path)
            return False

        img = Image.open(file_path)
        
        # 处理透明背景
        if img.mode in ('RGBA', 'LA', 'P'):
            background = Image.new('RGB', img.size, (255, 255, 255))
            if img.mode == 'P':
                img = img.convert('RGBA')
            background.paste(img, mask=img.split()[-1])
            img = background
        else:
            img = img.convert('RGB')

        # 调整尺寸
        width, height = img.size
        ratio = min(config.target_width / width, config.target_height / height)
        new_width = int(width * ratio)
        new_height = int(height * ratio)
        img = img.resize((new_width, new_height), Image.LANCZOS)

        # 强制转换为目标格式；Pillow库中JPG格式的标识符是'JPEG'而不是'JPG'，对于其他格式(PNG/GIF等)，直接使用大写格式名称即可
        new_file_path = os.path.splitext(file_path)[0] + '.' + config.target_format
        img.save(new_file_path, format='JPEG' if config.target_format.lower() == 'jpg' else config.target_format.upper())
        if new_file_path != file_path:
            os.remove(file_path)
        
        logging.info(f'已处理图片: {os.path.basename(file_path)} -> {os.path.basename(new_file_path)}')
        return True
        
    except Exception as e:
        logging.error(f'处理图片 {file_path} 时出错: {e}')
        return False


async def download_image(session, img_url, save_folder, start_index, downloaded_count, semaphore, config):
    async with semaphore:
        if should_skip(img_url, config):
            return None

        # 创建缓存文件夹
        cache_folder = os.path.join(save_folder, '.cache')
        if not os.path.exists(cache_folder):
            os.makedirs(cache_folder, exist_ok=True)

        max_retries = 3
        temp_filename = f"temp_{downloaded_count}.tmp"
        temp_path = os.path.join(cache_folder, temp_filename)
        
        for retry in range(max_retries):
            try:
                async with session.get(img_url, headers=config.headers, timeout=15) as response:
                    if response.status == 200:
                        async with aiofiles.open(temp_path, 'wb') as f:
                            while True:
                                chunk = await response.content.read(1024)
                                if not chunk:
                                    break
                                await f.write(chunk)
                        
                        await asyncio.sleep(0.1)
                        
                        file_size = os.path.getsize(temp_path)
                        if file_size >= config.min_file_size:
                            final_index = start_index + downloaded_count
                            
                            # 从URL中提取原始文件扩展名
                            parsed_url = urlparse(img_url)
                            path = parsed_url.path  # 先解析URL获取path
                            if not path:
                                logging.warning(f'URL {img_url} 中没有路径部分')
                                return None  # 跳过没有路径的URL

                            # 直接使用URL中的扩展名
                            ext = os.path.splitext(path)[1].lower()

                            
                            # 保留原始扩展名
                            final_path = os.path.join(save_folder, f'{config.keyword}_{final_index}{ext}')
                            
                            if os.path.exists(final_path):
                                os.remove(final_path)
                            os.rename(temp_path, final_path)
                            return final_path
                        else:
                            logging.info(f'{img_url} 图片小于最小尺寸要求')
                    else:
                        logging.warning(f'下载失败，状态码: {response.status}')
            except Exception as e:
                logging.error(f'下载错误: {e}')
                if retry < max_retries - 1:
                    await asyncio.sleep(2)
            finally:
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except:
                        pass
        return None

async def extract_img_urls(session, url, config, max_pages=5):  
    """
    从Bing图片搜索结果页提取图片URL
    
    Args:
        session: aiohttp会话对象
        url: 搜索URL
        config: 配置对象
        max_pages: 最大搜索页数
        
    Returns:
        list: 图片URL列表
    """
    img_urls = []
    # 修改循环范围为 range(config.max_pages) 使用配置参数
    for page in range(config.max_pages):  # 从0开始计数
        try:
            page_url = f"{url.split('&first=')[0]}&first={page*30}"
            async with session.get(page_url, headers=config.headers) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')
                    img_tags = soup.find_all('img', class_='mimg')
                    for img in img_tags:
                        img_url = img.get('src') or img.get('data-src')
                        if img_url:
                            if not img_url.startswith('http'):
                                base_url = 'https://cn.bing.com'
                                img_url = urllib.parse.urljoin(base_url, img_url)
                            img_urls.append(img_url)
                else:
                    logging.warning(f'请求搜索页面失败，状态码: {response.status}')
                    break
        except Exception as e:
            logging.error(f'提取图片链接时发生错误: {e}')
            break
    return img_urls

def rename_files(folder, config):
    """
    整理文件名格式为: 关键词_序号
    
    Args:
        folder: 图片文件夹路径
        config: 配置对象
    """
    try:
        # 获取所有jpg文件并按修改时间排序
        files = [f for f in os.listdir(folder) if f.endswith(config.target_format)]  # 错误行
        files.sort(key=lambda x: os.path.getmtime(os.path.join(folder, x)))
        
        # 检查重复文件(根据文件大小)
        file_sizes = {}
        duplicates = set()
        for filename in files:
            file_path = os.path.join(folder, filename)
            file_size = os.path.getsize(file_path)
            if file_size in file_sizes:
                duplicates.add(file_path)
            else:
                file_sizes[file_size] = file_path
        
        # 删除重复文件
        for dup_file in duplicates:
            os.remove(dup_file)
            logging.info(f'删除重复文件: {dup_file}')
            files.remove(os.path.basename(dup_file))
        
        # 重命名文件
        for index, filename in enumerate(files, start=1):
            old_path = os.path.join(folder, filename)
            new_name = f'{config.keyword}_{index}.{config.target_format}'
            new_path = os.path.join(folder, new_name)
            
            if old_path != new_path:
                if os.path.exists(new_path):
                    os.remove(new_path)
                os.rename(old_path, new_path)
                logging.info(f'重命名: {filename} -> {new_name}')
    except Exception as e:
        logging.error(f'整理文件名时出错: {e}')

async def main():
    import argparse
    parser = argparse.ArgumentParser(description='图片下载程序')
    # 修改后的参数定义（删除重复的--max_pages）
    parser.add_argument('--keyword', type=str, help='搜索关键词')
    parser.add_argument('--min_file_size', type=int, help='最小文件大小（KB）')
    parser.add_argument('--target_width', type=int, help='目标图片宽度')
    parser.add_argument('--target_height', type=int, help='目标图片高度')
    parser.add_argument('--max_pages', type=int, default=Config().max_pages, help='最大搜索页数(每页约30张)')
    parser.add_argument('--target_folder', type=str, default='train_data', help='目标存储文件夹')
    args = parser.parse_args()

    config = Config()
    config.update_from_args(args)

    save_folder, start_index, url = setup_environment(config)
    # 增加并发数到10 (原为5)
    semaphore = asyncio.Semaphore(10)  

    async with aiohttp.ClientSession() as session:
        img_urls = await extract_img_urls(session, url, config, max_pages=config.max_pages)  # 移除start_page参数

    tasks = []
    async with aiohttp.ClientSession() as session:
        # 添加进度计数器
        total = len(img_urls)
        completed = 0
        
        for index, img_url in enumerate(img_urls):
            # 检查URL是否已下载过
            url_log_path = os.path.join(save_folder, 'downloaded_urls.log')
            if os.path.exists(url_log_path):
                with open(url_log_path, 'r') as f:
                    if img_url in set(line.strip() for line in f if line.strip()):
                        logging.info(f'跳过已下载的URL: {img_url}')
                        continue
                        
            task = asyncio.create_task(
                download_image(session, img_url, save_folder, start_index, index, semaphore, config))
            
            def callback(future):
                nonlocal completed
                completed += 1
                logging.info(f'下载进度: {completed}/{total} ({(completed/total)*100:.1f}%)')
                
            task.add_done_callback(callback)
            tasks.append(task)

        downloaded_files = await asyncio.gather(*tasks)
        valid_files = [file for file in downloaded_files if file is not None]

    for file in valid_files:
        process_image(file, config)  

    rename_files(save_folder, config)
    
    # 下载完成后删除.cache文件夹
    cache_folder = os.path.join(save_folder, '.cache')
    if os.path.exists(cache_folder):
        import shutil
        shutil.rmtree(cache_folder)
        logging.info(f'已删除缓存文件夹: {cache_folder}')


if __name__ == "__main__":
    asyncio.run(main())

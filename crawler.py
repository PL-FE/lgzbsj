import requests
import json
import pandas as pd
from datetime import datetime, timedelta
import time
import re
import shutil
import os
from playwright.sync_api import sync_playwright
from openpyxl import load_workbook

# 接口2配置（预约数据）
URL_DETAIL = 'https://channels.weixin.qq.com/micro/statistic/cgi-bin/mmfinderassistant-bin/statistic/live_single_data'

# 接口3配置（带货商品的数据）
URL_PRODUCT = 'https://channels.weixin.qq.com/micro/statistic/cgi-bin/mmfinderassistant-bin/statistic/get_single_live_ec_spu_data_page_v2'

# 接口1配置（列表数据）
# 已将请求 URL 固定为下面的值
URL_LIST = 'https://channels.weixin.qq.com/micro/statistic/cgi-bin/mmfinderassistant-bin/live/get_live_history'

# 接口4配置（带货数据的整体转换数据）
URL_EC_SUMMARY = 'https://channels.weixin.qq.com/micro/statistic/cgi-bin/mmfinderassistant-bin/statistic/get_single_live_funnel'

# HEADERS 直接写死为空（可由浏览器会话覆盖）
HEADERS = {}

# COOKIES 直接写死为空（可由浏览器会话覆盖）
COOKIES_DICT = {}


 
# 浏览器 profile 目录（用于从持久化上下文读取 cookies / UA）
BROWSER_USER_DATA_DIR = './browser_data'

def get_browser_session_cookies_and_headers(user_data_dir=BROWSER_USER_DATA_DIR, url=None):
    """
    从 Playwright 的持久化上下文读取 cookies 和 User-Agent，返回 (headers_dict, cookies_dict)
    如果失败返回 ({}, {})
    """
    try:
        playwright = sync_playwright().start()
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,
            headless=True,
        )

        pages = context.pages
        page = pages[0] if pages else context.new_page()

        # 如果提供了 url，尝试打开以便获得对应域的 cookies
        if url:
            try:
                page.goto(url, wait_until='domcontentloaded', timeout=5000)
            except:
                pass

        # 获取 cookies 列表并转换为 dict
        try:
            cookies_list = context.cookies()
            cookies = {c.get('name'): c.get('value') for c in cookies_list}
        except:
            cookies = {}

        # 获取 User-Agent
        try:
            ua = page.evaluate("() => navigator.userAgent")
            headers = {}
            if ua:
                headers['User-Agent'] = ua
        except:
            headers = {}

        try:
            context.close()
        except:
            pass
        try:
            playwright.stop()
        except:
            pass

        return headers, cookies
    except Exception as e:
        print(f"  [Warning] 从浏览器会话获取 cookies/headers 失败: {e}")
        try:
            playwright.stop()
        except:
            pass
        return {}, {}

def get_time_range_for_half_year(start_date_str=None, end_date_str=None):
    """获取时间范围

    Args:
        start_date_str: 开始日期字符串，格式为 'YYYY-MM-DD'，默认为今年1月1号
        end_date_str: 结束日期字符串，格式为 'YYYY-MM-DD'，默认为当前时间

    Returns:
        tuple: (start_time, end_time) 时间戳
    """
    if start_date_str:
        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            start_time = int(start_date.timestamp())
        except ValueError:
            print(f"无效的开始日期格式: {start_date_str}，使用默认值")
            start_date = datetime(2025, 1, 1, 0, 0, 0)
            start_time = int(start_date.timestamp())
    else:
        # 默认从今年1月1号开始
        start_date = datetime(2025, 1, 1, 0, 0, 0)
        start_time = int(start_date.timestamp())

    if end_date_str:
        try:
            end_date = datetime.strptime(end_date_str + ' 23:59:59', '%Y-%m-%d %H:%M:%S')
            end_time = int(end_date.timestamp())
        except ValueError:
            print(f"无效的结束日期格式: {end_date_str}，使用当前时间")
            end_time = int(time.time())
    else:
        end_time = int(time.time())  # 当前时间戳

    return start_time, end_time

def fetch_live_data(page_size=10, current_page=1, start_time=None, end_time=None, headers=None, cookies=None):
    """获取单页数据"""
    if start_time is None or end_time is None:
        start_time, end_time = get_time_range_for_half_year()
    
    payload = {
        "pageSize": page_size,
        "currentPage": current_page,
        "reqType": 2,
        "filterStartTime": start_time,
        "filterEndTime": end_time,
        "timestamp": str(int(time.time() * 1000)),
        "rawKeyBuff": None,
        "pluginSessionId": None,
        "scene": 7,
        "reqScene": 7
    }
    
    # 优先使用传入的 headers/cookies（来自浏览器会话），否则回退到写死的 HEADERS/COOKIES_DICT
    request_headers = headers if headers is not None and headers else HEADERS
    request_cookies = cookies if cookies is not None and cookies else COOKIES_DICT

    try:
        response = requests.post(
            URL_LIST,
            json=payload,
            headers=request_headers,
            cookies=request_cookies,
            timeout=10
        )
        response.raise_for_status()
        data = response.json()
        
        if data.get('errCode') == 0:
            return data.get('data', {})
        else:
            print(f"API error: {data.get('errMsg')}")
            return None
    except Exception as e:
        print(f"Request error: {e}")
        return None

def fetch_detail_data(live_object_id):
    """获取直播详情数据（已弃用，保留以支持API方式）"""
    return None

def get_reserve_data_from_browser(page, live_object_id):
    """从浏览器页面获取预约数据（使用Playwright，等待页面内容自动完成）"""
    try:
        url = f"{URL_DETAIL}{live_object_id}"
        print(f"  访问URL: {url}")
        
        # 访问页面，等待DOM加载完成（不等待网络空闲，避免持续请求导致卡住）
        try:
            
            page.goto(url, wait_until='networkidle', timeout=30000)
            print("  页面DOM加载完成")
        except Exception as e:
            print(f"  [Warning] 页面加载超时，继续尝试: {e}")
        
        # 智能等待页面内容加载完成
        print("  检测页面加载状态...")
        max_wait_time = 10  # 最多等待10秒
        wait_interval = 0.5  # 每0.5秒检查一次
        waited_time = 0
        data_found = False
        
        while waited_time < max_wait_time:
            try:
                # 检查页面是否包含数据元素
                page_source = page.content()
                
                # 尝试查找预约相关的关键词，如果找到就立即继续
                if '预约人数' in page_source or '预约转化率' in page_source:
                    data_found = True
                    print(f"  检测到预约数据，立即继续（等待时间: {waited_time:.1f}秒）")
                    break
                
                # 如果页面已经加载完成（包含一些关键内容），也可以继续
                if '直播' in page_source and len(page_source) > 5000:
                    # 再等待一小段时间确保数据渲染完成
                    time.sleep(0.5)
                    data_found = True
                    print(f"  页面已加载完成，继续提取数据（等待时间: {waited_time:.1f}秒）")
                    break
                
                # 如果还没找到，继续等待
                time.sleep(wait_interval)
                waited_time += wait_interval
                
            except Exception as e:
                print(f"  [Warning] 检查页面时出错: {e}")
                break
        
        if not data_found:
            print(f"  页面加载超时，继续尝试提取数据...")
        
        reserve_data = {}
        
        # 提取当前URL
        try:
            current_url = page.url
            reserve_data['当前url'] = current_url
            print(f"  当前URL: {current_url}")
        except Exception as e:
            print(f"  [Warning] 获取当前URL失败: {e}")
            reserve_data['当前url'] = ''
        
        # 提取标题
        try:
            title_elem = page.query_selector('.live-build-info .content .title .text-wrap')
            if title_elem:
                title_text = title_elem.text_content().strip()
                reserve_data['标题'] = title_text
                print(f"  找到标题: {title_text}")
            else:
                # 如果CSS选择器没找到，尝试从页面源码中提取
                page_source = page.content()
                # 尝试从 .live-build-info .content .title .text-wrap 提取
                patterns = [
                    r'<[^>]*class="[^"]*live-build-info[^"]*"[^>]*>.*?<[^>]*class="[^"]*content[^"]*"[^>]*>.*?<[^>]*class="[^"]*title[^"]*"[^>]*>.*?<[^>]*class="[^"]*text-wrap[^"]*"[^>]*>([^<]+)</[^>]*>',
                    r'class="text-wrap"[^>]*>([^<]+)</[^>]*>',
                ]
                for pattern in patterns:
                    match = re.search(pattern, page_source, re.DOTALL)
                    if match:
                        reserve_data['标题'] = match.group(1).strip()
                        print(f"  从源码找到标题: {reserve_data['标题']}")
                        break
                else:
                    reserve_data['标题'] = ''
        except Exception as e:
            print(f"  [Warning] 提取标题失败: {e}")
            reserve_data['标题'] = ''

        # 提取预约人数和预约转化率
        try:
            # 提取预约人数（字段1）和预约转化率（字段2）
            try:
                # 查找预约人数和预约转化率
                summary_labels = page.query_selector_all('.live-data-card-summary-label')
                summary_values = page.query_selector_all('.live-data-card-summary-value')

                for label, value in zip(summary_labels, summary_values):
                    try:
                        label_text = label.text_content().strip()
                        value_text = value.text_content().strip()

                        if '预约人数' in label_text:
                            reserve_data['预约人数'] = value_text.strip()
                            print(f"  找到预约人数: {reserve_data['预约人数']}")
                        elif '预约转化率' in label_text:
                            reserve_data['预约转化率'] = value_text.strip()
                            print(f"  找到预约转化率: {reserve_data['预约转化率']}")
                    except:
                        continue
            except Exception as e:
                print(f"  [Warning] CSS选择器提取失败: {e}")

            # 如果CSS选择器没找到，从页面源码中提取预约人数和预约转化率
            page_source = page.content()

            # 提取预约人数
            if '预约人数' not in reserve_data:
                pattern = r'<div[^>]*class="live-data-card-summary-label"[^>]*>预约人数</div>\s*<div[^>]*class="live-data-card-summary-value"[^>]*>\s*([0-9,\s]+)\s*</div>'
                match = re.search(pattern, page_source)
                if match:
                    reserve_data['预约人数'] = match.group(1).strip()
                    print(f"  从源码找到预约人数: {reserve_data['预约人数']}")

            # 提取预约转化率
            if '预约转化率' not in reserve_data:
                pattern = r'<div[^>]*class="live-data-card-summary-label"[^>]*>预约转化率</div>\s*<div[^>]*class="live-data-card-summary-value"[^>]*>\s*([0-9.]+%)\s*</div>'
                match = re.search(pattern, page_source)
                if match:
                    reserve_data['预约转化率'] = match.group(1).strip()
                    print(f"  从源码找到预约转化率: {reserve_data['预约转化率']}")

            # 动态提取所有来源数据（从 reverse-data-legends 下提取）
            try:
                # 查找所有预约来源数据
                legend_items = page.query_selector_all('.reverse-data-legend')

                for item in legend_items:
                    try:
                        # 提取来源名称和数量
                        name_elem = item.query_selector('.reverse-data-legend-name')
                        count_elem = item.query_selector('.reverse-data-legend-count')

                        if name_elem and count_elem:
                            name = name_elem.text_content().strip()
                            count = count_elem.text_content().strip()

                            # 动态添加字段（使用来源名称作为字段名）
                            reserve_data[name] = count
                            print(f"  找到来源数据: {name} = {count}")
                    except Exception as e:
                        continue
            except Exception as e:
                print(f"  [Warning] CSS选择器提取来源数据失败: {e}")

            # 如果CSS选择器没找到，从页面源码中提取来源数据
            if 'reverse-data-legends' in page_source or True:  # 总是尝试从源码提取，确保完整性
                # 使用正则表达式提取所有来源数据
                legend_pattern = r'<div[^>]*class="reverse-data-legend-name"[^>]*>([^<]+)</div>\s*<div[^>]*class="reverse-data-legend-count"[^>]*>([0-9,\s]+)</div>'
                matches = re.findall(legend_pattern, page_source)

                for name, count in matches:
                    name = name.strip()
                    count = count.strip()

                    # 动态添加字段（如果还没有添加）
                    if name not in reserve_data:
                        reserve_data[name] = count
                        print(f"  从源码找到: {name} = {count}")

            # 设置默认值（只对预约人数和预约转化率）
            if '预约人数' not in reserve_data:
                reserve_data['预约人数'] = ''
            if '预约转化率' not in reserve_data:
                reserve_data['预约转化率'] = ''

        except Exception as e:
            print(f"  [Warning] 提取数据失败: {e}")
            reserve_data = {
                '预约人数': '',
                '预约转化率': ''
            }

        return reserve_data

    except Exception as e:
        print(f"  [Error] 获取页面失败: {e}")
        return None

def is_reserve_data_valid(reserve_data):
    """检查预约数据是否有效

    Args:
        reserve_data: 预约数据字典

    Returns:
        bool: True表示数据有效，False表示数据无效（需要重试）
    """
    if reserve_data is None:
        return False

    # 检查关键字段：预约人数必须存在且不为空
    reserve_count = reserve_data.get('预约人数', '').strip()
    if not reserve_count:
        return False

    return True

def get_product_data_from_browser(page, live_object_id):
    """从浏览器页面获取产品表格数据（使用Playwright，等待页面内容自动完成）"""
    try:
        url = f"{URL_PRODUCT}{live_object_id}&entrance_id=3&tab=product"
        print(f"  访问URL: {url}")

        # 访问页面，等待DOM加载完成
        try:
            page.goto(url, wait_until='networkidle', timeout=30000)
            print("  页面DOM加载完成")
        except Exception as e:
            print(f"  [Warning] 页面加载超时，继续尝试: {e}")

        # 智能等待页面内容加载完成
        print("  检测页面加载状态...")
        max_wait_time = 10  # 最多等待10秒
        wait_interval = 0.5  # 每0.5秒检查一次
        waited_time = 0
        data_found = False

        while waited_time < max_wait_time:
            try:
                # 检查页面是否包含表格元素
                page_source = page.content()

                # 尝试查找表格相关的关键词
                if '.ant-table-header' in page_source or 'ant-table' in page_source:
                    # 再等待一小段时间确保数据渲染完成
                    time.sleep(0.5)
                    data_found = True
                    print(f"  检测到表格数据，立即继续（等待时间: {waited_time:.1f}秒）")
                    break

                # 如果页面已经加载完成（包含一些关键内容），也可以继续
                if len(page_source) > 5000:
                    time.sleep(0.5)
                    data_found = True
                    print(f"  页面已加载完成，继续提取数据（等待时间: {waited_time:.1f}秒）")
                    break

                # 如果还没找到，继续等待
                time.sleep(wait_interval)
                waited_time += wait_interval

            except Exception as e:
                print(f"  [Warning] 检查页面时出错: {e}")
                break

        if not data_found:
            print(f"  页面加载超时，继续尝试提取数据...")

        product_data = {}

        # 提取当前URL
        try:
            current_url = page.url
            product_data['当前url'] = current_url
            print(f"  当前URL: {current_url}")
        except Exception as e:
            print(f"  [Warning] 获取当前URL失败: {e}")
            product_data['当前url'] = ''

        # 提取表格字段名（从 .ant-table-scroll 里的 .ant-table-header.ant-table-hide-scrollbar 里的 tr 里的 th）
        try:
            # 先尝试从 .ant-table-scroll .ant-table-header.ant-table-hide-scrollbar tr th 中查找
            header_ths = page.query_selector_all('.ant-table-scroll .ant-table-header.ant-table-hide-scrollbar tr th')
            field_names = []

            if header_ths:
                for th in header_ths:
                    try:
                        text = th.text_content().strip()
                        # 只添加非空字段名
                        if text:
                            field_names.append(text)
                            print(f"  找到字段名: {text}")
                    except Exception as e:
                        print(f"  [Warning] 提取字段名失败: {e}")
                        continue

            # 如果CSS选择器没找到，从页面源码中提取
            if not field_names:
                page_source = page.content()
                # 使用正则表达式提取所有 th 标签中的文本
                # 先找到 .ant-table-header 区域
                header_pattern = r'<thead[^>]*class="[^"]*ant-table[^"]*"[^>]*>.*?</thead>'
                header_match = re.search(header_pattern, page_source, re.DOTALL)
                if header_match:
                    header_content = header_match.group(0)
                    # 从 header 区域提取所有 th
                    th_pattern = r'<th[^>]*>(.*?)</th>'
                    matches = re.findall(th_pattern, header_content, re.DOTALL)
                    for match in matches:
                        # 清理HTML标签，只保留文本
                        text = re.sub(r'<[^>]+>', ' ', match).strip()
                        # 清理多余的空白字符
                        text = ' '.join(text.split())
                        # 只添加非空字段名
                        if text:
                            field_names.append(text)
                            print(f"  从源码找到字段名: {text}")
                else:
                    # 如果找不到 thead，直接在整个页面中查找 th
                    th_pattern = r'<th[^>]*class="[^"]*ant-table[^"]*"[^>]*>(.*?)</th>'
                    matches = re.findall(th_pattern, page_source, re.DOTALL)
                    for match in matches:
                        # 清理HTML标签，只保留文本
                        text = re.sub(r'<[^>]+>', ' ', match).strip()
                        # 清理多余的空白字符
                        text = ' '.join(text.split())
                        # 只添加非空字段名
                        if text:
                            field_names.append(text)
                            print(f"  从源码找到字段名: {text}")

            product_data['_field_names'] = field_names
            print(f"  共找到 {len(field_names)} 个字段名")

        except Exception as e:
            print(f"  [Warning] 提取字段名失败: {e}")
            product_data['_field_names'] = []

        # 提取表格数据（从 .ant-table-body 中的 .ant-table-tbody 中的所有 tr 的 textContent()）
        try:
            tbody_trs = page.query_selector_all('.ant-table-body .ant-table-tbody tr')
            table_rows = []

            for tr in tbody_trs:
                try:
                    # 提取该行所有单元格的文本内容
                    tds = tr.query_selector_all('td')
                    row_cells = []
                    for td in tds:
                        cell_text = td.text_content().strip()
                        row_cells.append(cell_text)

                    if row_cells:  # 只添加非空行
                        # 将单元格内容组合成字符串（用制表符分隔，方便后续解析）
                        row_text = '\t'.join(row_cells)
                        table_rows.append(row_cells)  # 保存为列表，方便后续处理
                        print(f"  找到数据行: {len(row_cells)} 个单元格")
                except Exception as e:
                    print(f"  [Warning] 提取数据行失败: {e}")
                    continue

            # 如果CSS选择器没找到，从页面源码中提取
            if not table_rows:
                page_source = page.content()
                # 使用正则表达式提取所有 tr 标签
                tr_pattern = r'<tr[^>]*class="[^"]*ant-table[^"]*"[^>]*>(.*?)</tr>'
                tr_matches = re.findall(tr_pattern, page_source, re.DOTALL)
                for tr_match in tr_matches:
                    # 提取该行所有 td 标签中的文本
                    td_pattern = r'<td[^>]*>(.*?)</td>'
                    td_matches = re.findall(td_pattern, tr_match, re.DOTALL)
                    row_cells = []
                    for td_match in td_matches:
                        # 清理HTML标签，只保留文本
                        text = re.sub(r'<[^>]+>', ' ', td_match).strip()
                        # 清理多余的空白字符
                        text = ' '.join(text.split())
                        row_cells.append(text)

                    if row_cells:
                        table_rows.append(row_cells)
                        print(f"  从源码找到数据行: {len(row_cells)} 个单元格")

            product_data['_table_rows'] = table_rows
            print(f"  共找到 {len(table_rows)} 行数据")

        except Exception as e:
            print(f"  [Warning] 提取表格数据失败: {e}")
            product_data['_table_rows'] = []

        return product_data

    except Exception as e:
        print(f"  [Error] 获取页面失败: {e}")
        return None

def is_product_data_valid(product_data):
    """检查产品数据是否有效

    Args:
        product_data: 产品数据字典

    Returns:
        bool: True表示数据有效，False表示数据无效（需要重试）
    """
    if product_data is None:
        return False

    # 只要 product_data 不为 None（能获取到URL），就认为有效
    # 即使没有表格数据（0行），也要保存 liveobjectid 和 url
    return True

def flatten_product_data(live_object_id, product_data, remark=''):
    """将产品数据展平，动态提取所有字段

    Args:
        live_object_id: 直播对象ID
        product_data: 产品数据字典（包含字段名和数据行）
        remark: 备注信息（成功时为空，失败时为"失败"）

    Returns:
        list: 展平后的数据列表（每行数据一个字典）
    """
    if product_data is None:
        return []

    field_names = product_data.get('_field_names', [])
    table_rows = product_data.get('_table_rows', [])
    current_url = product_data.get('当前url', '').strip()

    flattened_data = []

    # 如果没有数据行，至少保存 liveobjectid 和 当前url
    if not table_rows:
        row_data = {
            'liveobjectid': str(live_object_id),  # 转换为字符串
            '当前url': current_url,
        }
        # 如果有字段名，为每个字段添加空值
        for field_name in field_names:
            row_data[field_name] = ''
        flattened_data.append(row_data)
        return flattened_data

    # 检查字段名和数据行列数是否匹配（用于警告）
    if table_rows:
        first_row_cell_count = len(table_rows[0]) if table_rows else 0
        if first_row_cell_count != len(field_names):
            print(f"  [Warning] 字段名数量({len(field_names)})与数据行列数({first_row_cell_count})不匹配")

    # 处理每一行数据
    for row_idx, row_cells in enumerate(table_rows):
        # row_cells 是一个列表，包含该行所有单元格的文本
        # 创建该行的数据字典
        row_data = {
            'liveobjectid': str(live_object_id),  # 转换为字符串
            '当前url': current_url,
        }

        # 将字段名和数据值一一对应（按照下标）
        # 如果数据值数量少于字段名，用空字符串填充
        # 如果数据值数量多于字段名，只取前len(field_names)个
        for col_idx, field_name in enumerate(field_names):
            if col_idx < len(row_cells):
                row_data[field_name] = row_cells[col_idx].strip()
            else:
                row_data[field_name] = ''

        # 如果还有多余的数据值，可以添加到额外的字段中
        if len(row_cells) > len(field_names):
            for extra_idx in range(len(field_names), len(row_cells)):
                row_data[f'字段{extra_idx + 1}'] = row_cells[extra_idx].strip()

        flattened_data.append(row_data)

    return flattened_data

def flatten_detail_data(live_object_id, detail_data, remark=''):
    """将详情数据展平，动态提取所有字段

    Args:
        live_object_id: 直播对象ID
        detail_data: 详情数据字典（包含预约人数、预约转化率和动态来源字段）
        remark: 备注信息（成功时为空，失败时为"失败"）
    """
    if detail_data is None:
        return None

    # 固定字段：liveObjectId、当前url、标题、预约人数、预约转化率、备注
    flat_data = {
        'liveObjectId': str(live_object_id),  # 转换为字符串
        '当前url': detail_data.get('当前url', '').strip() if isinstance(detail_data.get('当前url'), str) else str(detail_data.get('当前url', '')),
        '标题': detail_data.get('标题', '').strip() if isinstance(detail_data.get('标题'), str) else str(detail_data.get('标题', '')),
        '预约人数': detail_data.get('预约人数', '').strip() if isinstance(detail_data.get('预约人数'), str) else str(detail_data.get('预约人数', '')),
        '预约转化率': detail_data.get('预约转化率', '').strip() if isinstance(detail_data.get('预约转化率'), str) else str(detail_data.get('预约转化率', '')),
        '备注': remark,  # 备注字段
    }

    # 动态添加所有来源字段（排除已添加的固定字段）
    fixed_fields = {'当前url', '标题', '预约人数', '预约转化率', '备注'}
    for key, value in detail_data.items():
        if key not in fixed_fields:
            # 动态添加字段
            if isinstance(value, str):
                flat_data[key] = value.strip()
            else:
                flat_data[key] = str(value) if value else ''

    return flat_data

def create_failed_record(live_object_id, current_url='', title=''):
    """创建失败记录

    Args:
        live_object_id: 直播对象ID
        current_url: 当前URL（如果失败时能获取到）
        title: 标题（如果失败时能获取到）

    Returns:
        dict: 失败记录字典
    """
    return {
        'liveObjectId': str(live_object_id),
        '当前url': current_url,
        '标题': title,
        '预约人数': '',
        '预约转化率': '',
        '备注': '失败',
    }

def set_liveobjectid_as_text_format(excel_file, sheet_name='详情数据'):
    """设置Excel文件中liveObjectId列的格式为文本

    Args:
        excel_file: Excel文件路径
        sheet_name: 工作表名称
    """
    try:
        # 使用 openpyxl 打开文件
        wb = load_workbook(excel_file)
        if sheet_name not in wb.sheetnames:
            wb.close()
            return

        ws = wb[sheet_name]

        # 查找 liveObjectId 列的索引
        header_row = 1
        liveobjectid_col = None
        for col_idx, cell in enumerate(ws[header_row], 1):
            if cell.value == 'liveObjectId':
                liveobjectid_col = col_idx
                break

        if liveobjectid_col:
            # 设置该列所有数据单元格的格式为文本
            from openpyxl.cell.cell import Cell
            for row_idx in range(2, ws.max_row + 1):  # 从第2行开始（跳过表头）
                cell = ws.cell(row=row_idx, column=liveobjectid_col)
                if cell.value is not None:
                    # 将值转换为字符串并设置为文本格式
                    cell.value = str(cell.value)
                    cell.number_format = '@'  # '@' 表示文本格式

        wb.save(excel_file)
        wb.close()
    except Exception as e:
        print(f"  [Warning] 设置liveObjectId列格式为文本失败: {e}")

def save_records_to_excel_file(output_file, all_records, sheet_name='产品数据', id_column_name='liveobjectid', silent=False):
    """保存所有记录到Excel文件（覆盖写入，不追加）

    Args:
        output_file: Excel文件路径
        all_records: 所有记录字典列表（包含之前已保存的记录和新记录）
        sheet_name: 工作表名称
        id_column_name: ID列名称（用于设置文本格式）
        silent: 是否静默模式（不输出日志）

    Returns:
        bool: 是否成功保存
    """
    try:
        if not all_records:
            return True

        # 确保所有记录的 ID 列是字符串格式
        for record in all_records:
            if id_column_name in record:
                record[id_column_name] = str(record[id_column_name])

        # 创建DataFrame
        df = pd.DataFrame(all_records)

        # 确保 ID 列是字符串类型
        if id_column_name in df.columns:
            df[id_column_name] = df[id_column_name].astype(str)

        # 保存到Excel文件，使用 openpyxl 引擎以便后续设置格式
        with pd.ExcelWriter(output_file, engine='openpyxl', mode='w') as writer:
            df.to_excel(writer, index=False, sheet_name=sheet_name)

            # 获取工作表并设置 ID 列为文本格式
            ws = writer.sheets[sheet_name]
            # 查找 ID 列的索引
            header_row = 1
            id_col = None
            for col_idx, cell in enumerate(ws[header_row], 1):
                if cell.value == id_column_name:
                    id_col = col_idx
                    break

            if id_col:
                # 设置该列所有单元格的格式为文本（包括表头和数据）
                for row_idx in range(1, ws.max_row + 1):  # 包括表头
                    cell = ws.cell(row=row_idx, column=id_col)
                    if cell.value is not None:
                        cell.value = str(cell.value)
                    cell.number_format = '@'  # '@' 表示文本格式

        if not silent:
            print(f"  保存 {len(all_records)} 条记录到 {output_file}")

        return True

    except Exception as e:
        if not silent:
            print(f"  [Error] 保存记录到Excel文件失败: {e}")
        return False

def flatten_live_data(live_object):
    """将列表数据展平"""
    live_stats = live_object.get('liveStats', {})

    # 只保留需要的字段，按照表头顺序
    flat_data = {
        'liveObjectId': str(live_object.get('liveObjectId')),  # 转换为字符串
        '直播信息': live_object.get('description'),
        '直播时长': live_object.get('liveStats', {}).get('liveDurationInSeconds', 0),
        '观看人数': live_object.get('liveStats', {}).get('totalAudienceCount', 0),
        '最高在线': live_object.get('maxOnlineCount', 0),
        '总热度': live_object.get('hotQuota', 0),
        '成交金额': live_object.get('payedGmv', '0'),
    }

    return flat_data

def backup_file(file_path):
    """备份文件，如果文件存在则复制并重命名为_backup

    Args:
        file_path: 要备份的文件路径
    """
    if os.path.exists(file_path):
        # 生成备份文件名
        if file_path.endswith('.xlsx'):
            backup_path = file_path[:-5] + '_backup.xlsx'
        else:
            backup_path = file_path + '_backup'

        try:
            shutil.copy2(file_path, backup_path)
            print(f"[备份] 已备份文件: {backup_path}")
        except Exception as e:
            print(f"[Warning] 备份文件失败: {e}")

def _download_data_with_browser(
    output_file,
    data_type_name,
    get_data_func,
    is_data_valid_func,
    flatten_data_func,
    create_failed_record_func,
    sheet_name,
    id_column_name,
    user_data_dir='./browser_data',
    keep_browser_open=True
):
    """通用的浏览器数据下载函数（内部函数，供其他函数调用）

    Args:
        output_file: 输出文件名
        data_type_name: 数据类型名称（用于日志输出，如"预约数据"、"产品数据"）
        get_data_func: 获取数据的函数，接受 (page, live_object_id) 参数
        is_data_valid_func: 验证数据有效性的函数，接受 data 参数
        flatten_data_func: 展平数据的函数，接受 (live_object_id, data, remark) 参数，返回单个记录或记录列表
        create_failed_record_func: 创建失败记录的函数，接受 (live_object_id, **kwargs) 参数
        sheet_name: Excel工作表名称
        id_column_name: ID列名称（用于设置文本格式）
        user_data_dir: 浏览器数据目录
        keep_browser_open: 是否保持浏览器打开

    Returns:
        bool: 是否成功
    """
    # 添加年月日时分秒后缀到文件名
    date_suffix = datetime.now().strftime('%Y%m%d%H%M%S')
    if output_file.endswith('.xlsx'):
        output_file = output_file[:-5] + '_' + date_suffix + '.xlsx'
    else:
        output_file = output_file + '_' + date_suffix + '.xlsx'

    print(f"\n开始用浏览器下载{data_type_name}...")
    print(f"输出文件: {output_file}")

    # 先从xlsx1.xlsx读取liveObjectId列表
    try:
        # 尝试读取新工作表名称，如果不存在则回退到旧名称（向后兼容）
        try:
            df_list = pd.read_excel('xlsx1.xlsx', sheet_name='列表数据')
        except ValueError:
            # 如果新工作表名称不存在，尝试旧的工作表名称
            df_list = pd.read_excel('xlsx1.xlsx', sheet_name='直播数据')
        live_ids = [str(live_id) for live_id in df_list['liveObjectId'].tolist()]  # 确保全部转换为字符串


    except Exception as e:
        print(f"读取xlsx1.xlsx失败: {e}")
        return False

    # 配置浏览器 - 使用 Playwright
    playwright = sync_playwright().start()

    try:
        # 使用持久化上下文（persistent context）来保存登录状态
        # 这样关闭浏览器后，下次运行时会自动恢复登录状态
        context = playwright.chromium.launch_persistent_context(
            user_data_dir=user_data_dir,  # 保存浏览器数据的目录
            headless=False,  # 显示浏览器窗口
            viewport={'width': 1920, 'height': 1080},
            locale='zh-CN',
            timezone_id='Asia/Shanghai',
            args=[
                '--disable-blink-features=AutomationControlled',
                '--window-size=1920,1080',
            ]
        )

        # 获取第一个页面（持久化上下文会自动创建）
        pages = context.pages
        if pages:
            page = pages[0]
        else:
            page = context.new_page()

        print("[提示] 浏览器已启动（使用持久化上下文，登录状态会被保存）")
        print("[提示] 直接开始爬取数据...")

        # 维护所有数据的列表（用于实时保存）
        all_records = []

        # 遍历每个liveObjectId
        for idx, live_id in enumerate(live_ids, 1):
            print(f"[{idx}/{len(live_ids)}] 正在获取 {live_id} 的{data_type_name}...")

            # 重试逻辑：最多重试3次
            max_retries = 3
            data = None

            for retry in range(max_retries):
                try:
                    if retry > 0:
                        print(f"  第 {retry + 1} 次重试...")
                    data = get_data_func(page, live_id)

                    # 检查数据是否有效
                    if is_data_valid_func(data):
                        if retry > 0:
                            print(f"  重试成功！")
                        break  # 成功获取有效数据，退出重试循环
                    else:
                        # 数据无效或为空，需要重试
                        if retry < max_retries - 1:
                            print(f"  第 {retry + 1} 次尝试失败：未获取到有效数据，将在 {retry + 1} 秒后重试...")
                            time.sleep(retry + 1)  # 递增延迟：1秒、2秒
                        else:
                            print(f"  第 {retry + 1} 次尝试失败：未获取到有效数据")
                except Exception as e:
                    if retry < max_retries - 1:
                        print(f"  第 {retry + 1} 次尝试失败: {e}")
                        print(f"  将在 {retry + 1} 秒后重试...")
                        time.sleep(retry + 1)  # 递增延迟：1秒、2秒
                    else:
                        print(f"  第 {retry + 1} 次尝试失败: {e}")

            if is_data_valid_func(data):
                flattened_data = flatten_data_func(live_id, data, remark='')
                # 处理单个记录或记录列表
                if flattened_data:
                    # 检查实际表格数据行数（用于日志输出）
                    actual_table_rows = 0
                    if isinstance(data, dict) and '_table_rows' in data:
                        actual_table_rows = len(data.get('_table_rows', []))

                    # 检查是单个记录还是记录列表
                    if isinstance(flattened_data, list):
                        # 多条记录，追加到列表
                        all_records.extend(flattened_data)
                        if actual_table_rows > 0:
                            print(f"  成功: 获取到 {actual_table_rows} 行数据")
                        else:
                            print(f"  成功: 表格无数据，已保存基本信息（liveobjectid 和 url）")
                    else:
                        # 单条记录，追加到列表
                        all_records.append(flattened_data)
                        if actual_table_rows > 0:
                            print(f"  成功: 获取到数据")
                        else:
                            print(f"  成功: 表格无数据，已保存基本信息（liveobjectid 和 url）")

                    # 实时保存所有数据（覆盖写入）
                    if save_records_to_excel_file(output_file, all_records, sheet_name=sheet_name, id_column_name=id_column_name, silent=True):
                        pass  # 静默保存
                    else:
                        print(f"  保存数据失败")
                else:
                    # 如果展平后为空，至少保存 liveobjectid 和 url
                    print(f"  警告: 数据格式不正确，保存基本信息")
                    try:
                        current_url = page.url if hasattr(page, 'url') else ''
                    except:
                        current_url = ''

                    # 尝试从 data 中获取 url
                    if data and isinstance(data, dict):
                        current_url = data.get('当前url', current_url)

                    # 创建基本记录
                    basic_record = {id_column_name: str(live_id), '当前url': current_url}
                    all_records.append(basic_record)

                    # 保存
                    if save_records_to_excel_file(output_file, all_records, sheet_name=sheet_name, id_column_name=id_column_name, silent=True):
                        print(f"  已保存基本信息（liveobjectid 和 url）")
                    else:
                        print(f"  保存基本信息失败")
            else:
                print(f"  失败: 重试 {max_retries} 次后仍无法获取有效数据")
                # 创建失败记录并追加到列表
                try:
                    current_url = page.url
                except:
                    current_url = ''

                # 尝试获取其他可能的失败信息
                failed_kwargs = {'current_url': current_url}
                try:
                    # 尝试获取标题（如果页面有）
                    title_elem = page.query_selector('.live-build-info .content .title .text-wrap')
                    if title_elem:
                        failed_kwargs['title'] = title_elem.text_content().strip()
                except:
                    pass
                
                failed_record = create_failed_record_func(live_id, **failed_kwargs)
                # 失败记录可能是单个记录或记录列表
                if isinstance(failed_record, list):
                    all_records.extend(failed_record)
                else:
                    all_records.append(failed_record)
                
                # 实时保存所有数据（覆盖写入）
                if save_records_to_excel_file(output_file, all_records, sheet_name=sheet_name, id_column_name=id_column_name, silent=True):
                    print(f"  已保存失败记录")
                else:
                    print(f"  保存失败记录失败")
            
            time.sleep(5)  # 间隔5秒
        
    except Exception as e:
        print(f"[错误] 浏览器启动失败: {e}")
        print("请确保已安装 Playwright 浏览器:")
        print("运行命令: playwright install chromium")
        return False
    finally:
        # 根据参数决定是否关闭浏览器
        if keep_browser_open:
            print("\n[提示] 浏览器将保持打开状态，您可以继续使用")
            print("[提示] 如需关闭浏览器，请手动关闭窗口")
            print("[提示] 注意：关闭浏览器窗口后，下次运行程序时会重新打开")
            # 不关闭浏览器和 playwright，让它们保持运行
        else:
            # 关闭浏览器上下文和 playwright
            try:
                context.close()  # 关闭持久化上下文（会自动保存状态）
            except:
                pass
            playwright.stop()
    
    # 数据已实时保存，这里只需要提示完成
    print(f"\n数据爬取完成！所有数据已实时保存到 {output_file}")
    return True

def download_detail_data(output_file='xlsx2.xlsx', user_data_dir='./browser_data'):
    """下载预约数据（接口2）"""
    return download_api_data(
        output_file=output_file,
        data_type_name='预约数据',
        fetch_func=fetch_live_single_data,
        flatten_func=flatten_live_single_data,
        sheet_name='预约数据',
        id_column_name='liveObjectId',
        user_data_dir=user_data_dir
    )

def download_product_data(output_file='xlsx3.xlsx', user_data_dir='./browser_data'):
    """下载带货商品的数据（接口3）"""
    # 备份旧文件
    backup_file(output_file)

    print(f"开始下载直播商品SPU数据...")
    print(f"输出文件: {output_file}")

    all_records = []

    # 读取 liveObjectId 列表（来自 xlsx1.xlsx）
    try:
        # 尝试读取新工作表名称，如果不存在则回退到旧名称（向后兼容）
        try:
            df_list = pd.read_excel('xlsx1.xlsx', sheet_name='列表数据')
        except ValueError:
            # 如果新工作表名称不存在，尝试旧的工作表名称
            df_list = pd.read_excel('xlsx1.xlsx', sheet_name='直播数据')
        live_ids = [str(live_id) for live_id in df_list['liveObjectId'].tolist()]
    except Exception as e:
        print(f"读取 xlsx1.xlsx 失败: {e}")
        return False

    # 尝试从浏览器会话获取 headers/cookies（只做一次）
    browser_headers, browser_cookies = get_browser_session_cookies_and_headers(
        user_data_dir=user_data_dir,
        url=URL_DETAIL  # 使用接口2的URL来获取cookies
    )
    if browser_headers or browser_cookies:
        print("已从浏览器会话获取 cookies/headers，将用于接口请求")
    else:
        browser_headers, browser_cookies = None, None

    for idx, live_id in enumerate(live_ids, 1):
        print(f"[{idx}/{len(live_ids)}] 获取 {live_id} 的带货商品的数据...")
        data = fetch_spu_data(live_id, headers=browser_headers, cookies=browser_cookies)
        if data is None:
            print(f"  警告: 未获取到数据，保存空记录")
            rec = {'liveObjectId': str(live_id)}
            all_records.append(rec)
        else:
            rec_list = flatten_spu_data(live_id, data)
            all_records.extend(rec_list)

        # 每次请求间隔 1 秒，避免过快
        time.sleep(1)

        # 每 50 条实时保存一次，防止意外中断丢失数据
        if idx % 50 == 0:
            save_records_to_excel_file(output_file, all_records, sheet_name='产品数据', id_column_name='liveObjectId', silent=True)

    # 最终保存
    success = save_records_to_excel_file(output_file, all_records, sheet_name='产品数据', id_column_name='liveObjectId', silent=False)
    if success:
        print(f"直播商品SPU数据已保存到 {output_file}，共 {len(all_records)} 条记录")
        return True
    else:
        print("保存直播商品SPU数据失败")
        return False


def fetch_live_single_data(live_object_id, headers=None, cookies=None, timeout=10):
    """调用接口2，获取指定 liveObjectId 的预约数据汇总，返回 data 字典或 None"""
    payload = {
        "liveObjectId": str(live_object_id),
        "timestamp": str(int(time.time() * 1000)),
        "_log_finder_uin": None,
        "_log_finder_id": "v2_060000231003b20faec8c5e58e18c6d4c605ed31b0777108d955d806e1454ae22f3ddeb0baf6@finder",
        "rawKeyBuff": None,
        "pluginSessionId": None,
        "scene": 7,
        "reqScene": 7
    }

    request_headers = headers if headers is not None and headers else HEADERS
    request_cookies = cookies if cookies is not None and cookies else COOKIES_DICT

    try:
        resp = requests.post(URL_DETAIL, json=payload, headers=request_headers, cookies=request_cookies, timeout=timeout)
        resp.raise_for_status()
        j = resp.json()
        if j.get('errCode') == 0:
            return j.get('data', {})
        else:
            print(f"接口2返回错误: {j.get('errMsg')}")
            return None
    except Exception as e:
        print(f"请求接口2失败: {e}")
        return None


def fetch_ec_summary(live_object_id, headers=None, cookies=None, timeout=10):
    """调用接口4，获取指定 liveObjectId 的带货数据的整体转换数据，返回 data 字典或 None"""
    payload = {
        "liveObjectId": str(live_object_id),
        "timestamp": str(int(time.time() * 1000)),
        "_log_finder_uin": None,
        "_log_finder_id": None,
        "rawKeyBuff": None,
        "pluginSessionId": None,
        "scene": 7,
        "reqScene": 7
    }

    request_headers = headers if headers is not None and headers else HEADERS
    request_cookies = cookies if cookies is not None and cookies else COOKIES_DICT

    try:
        resp = requests.post(URL_EC_SUMMARY, json=payload, headers=request_headers, cookies=request_cookies, timeout=timeout)
        resp.raise_for_status()
        j = resp.json()
        if j.get('errCode') == 0:
            return j.get('data', {})
        else:
            print(f"接口4返回错误: {j.get('errMsg')}")
            return None
    except Exception as e:
        print(f"请求接口4失败: {e}")
        return None


def fetch_spu_data(live_object_id, headers=None, cookies=None, timeout=10):
    """调用接口3，获取指定 liveObjectId 的带货商品的数据，返回 data 字典或 None"""
    payload = {
        "liveObjectId": str(live_object_id),
        "offset": 0,
        "limit": 15,
        "spuType": 0,
        "spuThreshold": {
            "lowStock": "10",
            "unpaidOrder": "10",
            "newBuyerConv": "10"
        },
        "spuSrc": 0,
        "fieldList": [
            "stock", "create_pv", "pay_pv", "gmv", "clk_pay_ratio", "create_uv",
            "pay_uv", "new_customer_pay_pv", "no_finish_pv", "share_uv", "exp_uv",
            "exp_pv", "clk_uv", "clk_pv", "exp_clk_ratio", "clk_pay_ratio_pv",
            "new_customer_conversion_rate", "id", "explanation_count",
            "new_customer_conversion_rate_pv", "refund_rate", "refund_uv",
            "refund_pv", "refund_amount"
        ],
        "timestamp": str(int(time.time() * 1000)),
        "_log_finder_uin": None,
        "_log_finder_id": "v2_060000231003b20faec8c5e58e18c6d4c605ed31b0777108d955d806e1454ae22f3ddeb0baf6@finder",
        "rawKeyBuff": None,
        "pluginSessionId": None,
        "scene": 7,
        "reqScene": 7
    }

    request_headers = headers if headers is not None and headers else HEADERS
    request_cookies = cookies if cookies is not None and cookies else COOKIES_DICT

    try:
        resp = requests.post(URL_PRODUCT, json=payload, headers=request_headers, cookies=request_cookies, timeout=timeout)
        resp.raise_for_status()
        j = resp.json()
        if j.get('errCode') == 0:
            return j.get('data', {})
        else:
            print(f"接口3返回错误: {j.get('errMsg')}")
            return None
    except Exception as e:
        print(f"请求接口3失败: {e}")
        return None


def flatten_live_single_data(live_object_id, single_data):
    """将接口2的 data 展平为一条记录（dict），并确保所有值为字符串"""
    if single_data is None:
        return None

    flat = {'liveObjectId': str(live_object_id)}

    # 处理预约通知用户数和比率
    flat['reserveNoticeUserCount'] = str(single_data.get('reserveNoticeUserCount', ''))
    flat['reserveNoticeJoinliveRatio'] = str(single_data.get('reserveNoticeJoinliveRatio', ''))

    # 处理场景数组，将每个场景的数据作为单独字段
    # 遍历data中的所有项，找出数组类型的项（即场景数据）
    for key, value in single_data.items():
        if isinstance(value, list):
            # 这是场景数据数组
            for item in value:
                if isinstance(item, dict) and 'scene' in item and 'reserveNoticeUserCount' in item:
                    scene = item['scene']
                    count = item['reserveNoticeUserCount']
                    flat[f'scene_{scene}_reserveNoticeUserCount'] = str(count)

    return flat


def flatten_ec_summary(live_object_id, ec_data):
    """将接口4的 data 展平为一条记录（dict），并确保所有值为字符串"""
    if ec_data is None:
        return None

    flat = {'liveObjectId': str(live_object_id)}
    # 将 data 中的键值全部转为字符串并加入
    for k, v in ec_data.items():
        # 保持原始键名，值转换为字符串（None -> ''）
        if v is None:
            flat[k] = ''
        elif isinstance(v, (int, float, bool)):
            flat[k] = str(v)
        else:
            flat[k] = str(v).strip()

    return flat


def flatten_spu_data(live_object_id, spu_data):
    """将接口3的 data 展平为多条记录（list），每条记录的第一列是liveObjectId"""
    if spu_data is None:
        return []

    spu_data_list = spu_data.get('spuDataList', [])

    if not spu_data_list:
        # 如果没有数据，至少返回一条包含liveObjectId的记录
        return [{'liveObjectId': str(live_object_id)}]

    flattened_data = []

    for spu_item in spu_data_list:
        flat_record = {'liveObjectId': str(live_object_id)}

        # 处理baseData字段
        base_data = spu_item.get('baseData', {})
        if base_data:
            flat_record['srcSpuId'] = str(base_data.get('srcSpuId', ''))
            flat_record['spuId'] = str(base_data.get('spuId', ''))
            flat_record['src'] = str(base_data.get('src', ''))
            flat_record['spuName'] = str(base_data.get('spuName', ''))
            flat_record['thumbUrl'] = str(base_data.get('thumbUrl', ''))
            flat_record['price'] = str(base_data.get('price', ''))
            flat_record['srcName'] = str(base_data.get('srcName', ''))
            flat_record['baseStock'] = str(base_data.get('stock', ''))  # 重命名避免冲突

        # 处理其他字段
        for key, value in spu_item.items():
            if key not in ['baseData']:  # baseData已单独处理
                if value is None:
                    flat_record[key] = ''
                elif isinstance(value, (int, float, bool)):
                    flat_record[key] = str(value)
                else:
                    flat_record[key] = str(value).strip()

        flattened_data.append(flat_record)

    return flattened_data


def download_ec_summary(output_file='xlsx4.xlsx', user_data_dir='./browser_data'):
    """下载带货数据的整体转换数据（接口4）"""
    return download_api_data(
        output_file=output_file,
        data_type_name='带货数据的整体转换数据',
        fetch_func=fetch_ec_summary,
        flatten_func=flatten_ec_summary,
        sheet_name='EC汇总',
        id_column_name='liveObjectId',
        user_data_dir=user_data_dir
    )

def check_login_status(user_data_dir='./browser_data'):
    """检查登录状态，询问用户是否已登录，如果未登录则使用 Playwright 打开登录页面"""
    login_url = 'https://channels.weixin.qq.com/login.html'
    
    print("=" * 60)
    print("登录状态检查")
    print("=" * 60)
    
    while True:
        user_input = input("您是否已登录微信视频号助手？(y/n): ").strip().lower()
        
        if user_input in ['y', 'yes', '是', 'Y']:
            print("已确认登录状态，开始执行程序...")
            return True
        elif user_input in ['n', 'no', '否', 'N']:
            print(f"\n请先登录后再运行程序。")
            print(f"登录地址: {login_url}")
            print("\n正在使用 Playwright 打开登录页面...")
            
            # 使用 Playwright 打开登录页面
            playwright = sync_playwright().start()
            try:
                # 使用持久化上下文，这样登录状态会被保存
                context = playwright.chromium.launch_persistent_context(
                    user_data_dir=user_data_dir,
                    headless=False,
                    viewport={'width': 1920, 'height': 1080},
                    locale='zh-CN',
                    timezone_id='Asia/Shanghai',
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--window-size=1920,1080',
                    ]
                )
                
                # 获取第一个页面或创建新页面
                pages = context.pages
                if pages:
                    page = pages[0]
                else:
                    page = context.new_page()
                
                # 打开登录页面
                page.goto(login_url)
                print("登录页面已打开，请在浏览器中完成登录。")
                print("登录完成后，请关闭浏览器窗口，然后重新运行此程序。")
                print("\n按 Enter 键继续...")
                input()
                
                # 关闭浏览器上下文
                context.close()
                playwright.stop()
            except Exception as e:
                print(f"打开登录页面失败: {e}")
                print(f"请手动访问: {login_url}")
                try:
                    playwright.stop()
                except:
                    pass
            
            print("\n程序已退出。请登录后重新运行。")
            return False
        else:
            print("请输入 y 或 n（是/否）")

def download_api_data(
    output_file,
    data_type_name,
    fetch_func,
    flatten_func,
    sheet_name,
    id_column_name,
    user_data_dir='./browser_data',
    is_batch_request=False,
    batch_params=None
):
    """
    统一的API数据下载函数

    Args:
        output_file: 输出文件名
        data_type_name: 数据类型名称（用于日志输出）
        fetch_func: 数据获取函数
        flatten_func: 数据展平函数
        sheet_name: Excel工作表名称
        id_column_name: ID列名称
        user_data_dir: 浏览器数据目录
        is_batch_request: 是否为批量请求（如接口1的分页）
        batch_params: 批量请求的参数（仅当is_batch_request=True时使用）
    """
    # 备份旧文件
    backup_file(output_file)

    print(f"开始下载{data_type_name}...")
    print(f"输出文件: {output_file}")

    all_records = []

    # 对于批量请求（接口1），不需要读取xlsx1.xlsx，直接进行批量获取
    if is_batch_request:
        # 尝试从浏览器会话获取 headers/cookies（只做一次）
        browser_headers, browser_cookies = get_browser_session_cookies_and_headers(
            user_data_dir=user_data_dir,
            url=URL_LIST
        )
        if browser_headers or browser_cookies:
            print("已从浏览器会话获取 cookies/headers，将用于接口请求")
        else:
            browser_headers, browser_cookies = None, None

        # 批量请求处理（接口1）
        start_date = batch_params.get('start_date') if batch_params else None
        end_date = batch_params.get('end_date') if batch_params else None
        start_time, end_time = get_time_range_for_half_year(start_date, end_date)

        start_datetime = datetime.fromtimestamp(start_time)
        end_datetime = datetime.fromtimestamp(end_time)
        print(f"时间范围: {start_datetime.strftime('%Y-%m-%d %H:%M:%S')} 到 {end_datetime.strftime('%Y-%m-%d %H:%M:%S')}")
        if start_date or end_date:
            print(f"自定义时间范围: {start_date or '默认'} 到 {end_date or '当前时间'}")

        current_page = 1
        page_size = batch_params.get('page_size', 50) if batch_params else 50
        total_count = None

        while True:
            print(f"正在下载第 {current_page} 页...")

            result = fetch_func(
                page_size=page_size,
                current_page=current_page,
                start_time=start_time,
                end_time=end_time,
                headers=browser_headers,
                cookies=browser_cookies
            )

            if result is None:
                print(f"第 {current_page} 页下载失败，停止")
                break

            data_list = result.get('liveObjectList', [])

            if not data_list:
                print(f"第 {current_page} 页无数据，下载完成")
                break

            # 展平数据并添加到列表
            for data_obj in data_list:
                flat_obj = flatten_func(data_obj)
                all_records.append(flat_obj)

            # 获取总数
            if total_count is None:
                total_count = result.get('totalLiveCount', 0)
                print(f"总共有 {total_count} 条数据")

            print(f"已下载 {len(all_records)} 条数据")

            # 检查是否已下载所有数据
            if len(all_records) >= total_count:
                print(f"已获取所有 {total_count} 条数据")
                break

            current_page += 1
            time.sleep(1)  # 暂停1秒，避免请求过于频繁
    else:
        # 单条请求处理（接口2、4）- 需要先读取xlsx1.xlsx获取liveObjectId列表
        # 读取 liveObjectId 列表（来自 xlsx1.xlsx）
        try:
            # 尝试读取新工作表名称，如果不存在则回退到旧名称（向后兼容）
            try:
                df_list = pd.read_excel('xlsx1.xlsx', sheet_name='列表数据')
            except ValueError:
                # 如果新工作表名称不存在，尝试旧的工作表名称
                df_list = pd.read_excel('xlsx1.xlsx', sheet_name='直播数据')
            live_ids = [str(live_id) for live_id in df_list['liveObjectId'].tolist()]
        except Exception as e:
            print(f"读取 xlsx1.xlsx 失败: {e}")
            return False

        # 尝试从浏览器会话获取 headers/cookies（只做一次）
        browser_headers, browser_cookies = get_browser_session_cookies_and_headers(
            user_data_dir=user_data_dir,
            url=URL_LIST if data_type_name == '列表数据' else None
        )
        if browser_headers or browser_cookies:
            print("已从浏览器会话获取 cookies/headers，将用于接口请求")
        else:
            browser_headers, browser_cookies = None, None

        for idx, live_id in enumerate(live_ids, 1):
            print(f"[{idx}/{len(live_ids)}] 获取 {live_id} 的{data_type_name}...")
            data = fetch_func(live_id, headers=browser_headers, cookies=browser_cookies)
            if data is None:
                print(f"  警告: 未获取到数据，保存空记录")
                rec = {id_column_name: str(live_id)}
                all_records.append(rec)
            else:
                rec = flatten_func(live_id, data)
                all_records.append(rec)

            # 每次请求间隔 1 秒，避免过快
            time.sleep(1)

            # 每 50 条实时保存一次，防止意外中断丢失数据
            if idx % 50 == 0:
                save_records_to_excel_file(output_file, all_records, sheet_name=sheet_name, id_column_name=id_column_name, silent=True)

    # 最终保存
    success = save_records_to_excel_file(output_file, all_records, sheet_name=sheet_name, id_column_name=id_column_name, silent=False)
    if success:
        print(f"{data_type_name}已保存到 {output_file}，共 {len(all_records)} 条记录")
        return True
    else:
        print(f"保存{data_type_name}失败")
        return False

def download_half_year_data(output_file='xlsx1.xlsx', user_data_dir='./browser_data', start_date=None, end_date=None):
    """下载列表数据（接口1）

    Args:
        output_file: 输出文件名
        user_data_dir: 浏览器数据目录
        start_date: 开始日期，格式为 'YYYY-MM-DD'，默认为今年1月1号
        end_date: 结束日期，格式为 'YYYY-MM-DD'，默认为当前日期

    Examples:
        # 使用默认时间范围（今年1月1号到当前时间）
        download_half_year_data()

        # 指定时间范围
        download_half_year_data(start_date='2024-01-01', end_date='2024-12-31')

        # 只指定开始日期，结束日期使用当前时间
        download_half_year_data(start_date='2024-06-01')

        # 只指定结束日期，开始日期使用默认值（今年1月1号）
        download_half_year_data(end_date='2024-12-31')
    """
    return download_api_data(
        output_file=output_file,
        data_type_name='列表数据',
        fetch_func=fetch_live_data,
        flatten_func=flatten_live_data,
        sheet_name='列表数据',
        id_column_name='liveObjectId',
        user_data_dir=user_data_dir,
        is_batch_request=True,
        batch_params={
            'page_size': 50,
            'start_date': start_date,
            'end_date': end_date
        }
    )

if __name__ == '__main__':
    # 检查登录状态
    if not check_login_status():
        exit(0)

    print("\n" + "=" * 60)
    print("开始执行数据下载任务")
    print("=" * 60 + "\n")

    # 下载列表数据（接口1）
    print("正在下载列表数据（接口1）...")
    success1 = download_half_year_data()  # 使用默认时间范围（今年1月1号到当前时间）
    # download_half_year_data(start_date='2024-01-01', end_date='2024-12-31')  # 指定时间范围

    if success1:
        print("\n" + "=" * 60)
        print("列表数据下载完成，开始下载其他接口数据")
        print("=" * 60 + "\n")

        # 下载预约数据（接口2）- 下载全部数据
        # 输出文件名为 xlsx2.xlsx
        print("正在下载预约数据（接口2）...")
        download_detail_data()

        # 下载带货商品的数据（接口3）- 下载全部数据
        # 输出文件名为 xlsx3.xlsx
        print("正在下载带货商品的数据（接口3）...")
        download_product_data()

        # 下载带货数据的整体转换数据（接口4）
        # 输出文件名为 xlsx4_YYYYMMDDHHMMSS.xlsx
        print("正在下载带货数据的整体转换数据（接口4）...")
        download_ec_summary()

        print("\n" + "=" * 60)
        print("所有接口数据下载完成！")
        print("=" * 60 + "\n")
    else:
        print("\n" + "=" * 60)
        print("列表数据下载失败，跳过其他接口的下载")
        print("=" * 60 + "\n")

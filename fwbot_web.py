from telethon.tl.types import InputPeerUser, InputPeerChannel, PeerChat, PeerChannel
from telethon import TelegramClient, sync, events, errors
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession          # 新增导入
import sys, asyncio, traceback
import queue, time, json, os, re
from datetime import datetime, timezone, timedelta
import threading
import http.server
import collections

# ---------- 全局日志存储 ----------
log_buffer = collections.deque(maxlen=15)
log_lock = threading.Lock()

def log(msg):
    """打印并记录日志（附加北京时间）"""
    beijing_tz = timezone(timedelta(hours=8))
    now = datetime.now(timezone.utc).astimezone(beijing_tz).strftime('%Y-%m-%d %H:%M:%S')
    full_msg = f"[{now}] {msg}"
    with log_lock:
        log_buffer.append(full_msg)
    print(full_msg)


def configfile(file_path):
    """读取 JSON 文件并返回解析后的数据"""
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except FileNotFoundError:
        log(f"错误：文件 {file_path} 不存在")
    except json.JSONDecodeError as e:
        log(f"错误：JSON 解析失败 - {e}")
    except Exception as e:
        log(f"其他错误：{e}")


class TelegramMessageForwarder:
    def __init__(self, config_path):
        self.config_path = config_path
        self.private_group_id = None
        self.client = None

    def get_beijing_time(self, dt=None):
        """获取北京时间"""
        if dt is None:
            dt = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        beijing_tz = timezone(timedelta(hours=8))
        beijing_time = dt.astimezone(beijing_tz)
        return beijing_time.strftime('%Y-%m-%d %p %H:%M:%S')

    async def initialize(self):
        result = configfile(self.config_path)
        api_id = result['api_id']
        api_hash = result['api_hash']
        string_session = result.get('string_session')

        if not string_session:
            raise Exception("配置文件中缺少 string_session 字段，请提供有效的 StringSession")

        # 使用 StringSession 直接登录，无需交互认证
        self.client = TelegramClient(StringSession(string_session), api_id, api_hash)
        await self.client.connect()

        if not await self.client.is_user_authorized():
            raise Exception("StringSession 无效或已过期，请重新生成并更新配置文件")

        await self.get_private_group_id()

    async def get_private_group_id(self):
        result = configfile(self.config_path)
        private_group_username = result['private_group_username']

        async for dialog in self.client.iter_dialogs():
            if (dialog.is_channel or dialog.is_group) and private_group_username in dialog.title:
                log(f'找到目标群组: {dialog.title} (ID: {dialog.id})')
                self.private_group_id = dialog.id
                return
        raise Exception("未找到指定的私有群组")

    def build_condition(self):
        conditions = []
        result = configfile(self.config_path)
        condition_fw = result['condition_fw']

        for key in result["conditions_sth"]:
            if condition_fw == "yes":
                conditions.append(f"'{key}' in msg")
            else:
                conditions.append(f"'{key}' not in msg")

        for key, value in result["conditions_ext"].items():
            key_clean = key.strip("'\"")
            if value is None:
                val_repr = "None"
            elif isinstance(value, str):
                val_repr = f"'{value}'"
            elif isinstance(value, bool):
                val_repr = str(value)
            else:
                val_repr = str(value)

            if condition_fw == "yes":
                conditions.append(f"{key_clean} == {val_repr}")
            else:
                conditions.append(f"{key_clean} != {val_repr}")

        if condition_fw == "yes":
            return " or ".join(conditions)
        else:
            return " and ".join(conditions)

    async def handle_message(self, event):
        """统一处理新消息和编辑消息"""
        retries = 0
        max_retries = 5

        while retries < max_retries:
            try:
                msg = event.message.raw_text
                log(f'收到消息: {datetime.now()}, 内容: {msg}')
                if_condition = self.build_condition()
                log(f'{datetime.now()} if_condition 过滤条件: {if_condition}')
                if eval(if_condition):
                    log("符合转发条件，正在转发...")
                    await self.client.forward_messages(
                        self.private_group_id,
                        event.message.id,
                        from_peer=event.message.peer_id
                    )
                    event_time = self.get_beijing_time(event.date)
                    current_time = self.get_beijing_time()
                    log(f"转发完成 {event_time} {current_time}")
                else:
                    log("不符合转发条件，跳过")
                break

            except FloodWaitError as e:
                log(f"触发 Flood 限制，需要等待 {e.seconds} 秒")
                await asyncio.sleep(e.seconds + 5)
            except Exception as e:
                log(f"处理消息时发生错误: {e}. 正在重试 ({retries + 1}/{max_retries})...")
                retries += 1
                traceback.print_exc()
                await asyncio.sleep(3)

    async def start_monitoring(self):
        """开始监控消息（带连接状态打印）"""
        result = configfile(self.config_path)
        channel_username = result['channel_username']

        @self.client.on(events.NewMessage(chats=[channel_username]))
        async def new_message_handler(event):
            await self.handle_message(event)

        @self.client.on(events.MessageEdited(chats=[channel_username]))
        async def edited_message_handler(event):
            await self.handle_message(event)

        async def print_connection_status():
            while True:
                await asyncio.sleep(30)
                if self.client.is_connected():
                    current_time = self.get_beijing_time()
                    log(f"{current_time}✅ Telegram 连接正常")
                else:
                    current_time = self.get_beijing_time()
                    log(f"{current_time}❌ Telegram 连接已断开")

        status_task = asyncio.create_task(print_connection_status())
        log("开始监控消息...")
        try:
            await self.client.run_until_disconnected()
        finally:
            status_task.cancel()


# ---------- HTTP 请求处理器 ----------
class StatusHandler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/status':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain; charset=utf-8')
            self.end_headers()
            with log_lock:
                logs = list(log_buffer)
            if not logs:
                self.wfile.write("暂无日志\n".encode('utf-8'))
            else:
                self.wfile.write('\n'.join(logs).encode('utf-8'))
                self.wfile.write('\n'.encode('utf-8'))
        else:
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Not Found')

    def log_message(self, format, *args):
        # 抑制 HTTP 服务器自身的日志输出，避免干扰主日志
        pass


def start_http_server():
    server = http.server.HTTPServer(('localhost', 8080), StatusHandler)
    log("HTTP 状态服务已启动：http://localhost:8080/status")
    server.serve_forever()


def main():
    script_path = os.path.abspath(__file__)
    directory = os.path.dirname(script_path)
    log(f"当前脚本路径: {directory}")

    config_path = os.path.join(directory, "fwbot.json")

    # 启动 HTTP 服务器线程
    http_thread = threading.Thread(target=start_http_server, daemon=True)
    http_thread.start()

    async def run():
        forwarder = TelegramMessageForwarder(config_path)
        await forwarder.initialize()
        await forwarder.start_monitoring()

    asyncio.run(run())


if __name__ == "__main__":
    main()

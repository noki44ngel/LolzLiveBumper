import asyncio
import sys
import time
import signal
import random
import aiohttp
from datetime import datetime
from loguru import logger
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties

from config import (
    API_BASE_URL, API_TOKEN, THREAD_IDS,
    BUMP_INTERVAL, CHECK_INTERVAL, API_DELAY,
    RETRY_DELAYS, MAX_CONSECUTIVE_FAILURES,
    TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, ADMIN_USER_IDS, USER_AGENT
)
from database import Database
dp = Dispatcher()
bot = None
if TELEGRAM_BOT_TOKEN and 'YOUR_' not in TELEGRAM_BOT_TOKEN:
    bot = Bot(token=TELEGRAM_BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))

class AsyncBumpService:
    def __init__(self):
        self.db = Database()
        self.headers = {
            'Authorization': f'Bearer {API_TOKEN}',
            'User-Agent': USER_AGENT
        }
        self.running = True
        self._session = None
    
    async def get_session(self):
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers=self.headers)
        return self._session
    
    async def close(self):
        self.running = False
        if self._session:
            await self._session.close()
            
    async def _get_thread_info(self, thread_id: int) -> dict | None:
        url = f'{API_BASE_URL}/threads/{thread_id}'
        session = await self.get_session()
        try:
            async with session.get(url, timeout=30) as response:
                if response.status == 200:
                    return await response.json()
                elif response.status == 429:
                    logger.warning("Rate limit hit")
                return None
        except Exception as e:
            logger.error(f"Error fetching info: {e}")
            return None

    async def initialize_threads(self):
        now = int(time.time())
        config_ids = set(THREAD_IDS)
        
        db_threads = self.db.get_all_threads()
        db_ids = {t['thread_id'] for t in db_threads}
        for thread_id in config_ids:
            if thread_id not in db_ids:
                info = await self._get_thread_info(thread_id)
                title = 'Unknown'
                next_bump = now
                
                if info:
                    title = info.get('thread', {}).get('thread_title', 'Unknown')
                    bump_info = info.get('thread', {}).get('permissions', {}).get('bump', {})
                    if bump_info.get('next_available_time'):
                        next_bump = bump_info['next_available_time']
                
                self.db.upsert_thread(thread_id, title=title, next_bump_time=next_bump)
                logger.info(f"Added new thread: {title}")
                await asyncio.sleep(API_DELAY)
            else:
                thread = next(t for t in db_threads if t['thread_id'] == thread_id)
                if not thread.get('is_active'):
                    self.db.activate_thread(thread_id)
                    logger.info(f"Re-activated thread {thread_id}")

        for thread in db_threads:
            tid = thread['thread_id']
            if tid not in config_ids and thread.get('is_active'):
                self.db.deactivate_thread(tid)
                logger.info(f"Deactivated thread {tid} (removed from config)")

    async def _bump_thread(self, thread_id: int) -> dict:
        url = f'{API_BASE_URL}/threads/{thread_id}/bump'
        session = await self.get_session()
        result = {'success': False, 'message': '', 'next_time': None}
        
        try:
            async with session.post(url, timeout=30) as response:
                data = await response.json()
                
                if response.status == 200 and data.get('status') == 'ok':
                    result['success'] = True
                    result['message'] = data.get('message', 'Bumped')
                    jitter = random.randint(30, 300)
                    result['next_time'] = int(time.time()) + BUMP_INTERVAL + jitter
                    logger.debug(f"Applied jitter: +{jitter}s")
                    
                elif response.status == 403 and 'errors' in data:
                    result['message'] = data['errors'][0]
                    result['is_cooldown'] = True
                    info = await self._get_thread_info(thread_id)
                    if info:
                        bump = info.get('thread', {}).get('permissions', {}).get('bump', {})
                        if bump.get('next_available_time'):
                            result['next_time'] = bump['next_available_time']
                    if not result['next_time']:
                        result['next_time'] = int(time.time()) + BUMP_INTERVAL
                        
                elif response.status == 429:
                    result['message'] = 'Rate Limit'
                    result['next_time'] = int(time.time()) + 120
                    
                elif response.status == 401:
                    logger.critical("INVALID TOKEN")
                    self.running = False
                    if bot: await bot.send_message(TELEGRAM_CHAT_ID, "🚨 <b>CRITICAL: Invalid Token!</b>")
                    return result

                else:
                    result['message'] = f"Error {response.status}"
                    
        except Exception as e:
            result['message'] = str(e)
            
        return result

    async def process_thread(self, thread: dict) -> dict:
        thread_id = thread['thread_id']
        title = thread.get('title', str(thread_id))
        consecutive_failures = thread.get('consecutive_failures', 0)
        
        logger.info(f"Processing: {title}")
        result = await self._bump_thread(thread_id)
        result['title'] = title
        result['thread_id'] = thread_id
        
        if result['success']:
            logger.success(f"Bumped {thread_id}")
            self.db.record_bump_success(thread_id, result['message'], result['next_time'])
            
        elif result.get('is_cooldown'):
            logger.warning(f"Cooldown {thread_id}")
            self.db.upsert_thread(thread_id, next_bump_time=result['next_time'], last_error=result['message'], consecutive_failures=0)
            
        else:
            consecutive_failures += 1
            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.error(f"Skipping {thread_id} (10 fails)")
                next_retry = int(time.time()) + BUMP_INTERVAL
                self.db.upsert_thread(
                    thread_id, next_bump_time=next_retry, 
                    last_error=f"Too many errors: {result['message']}", 
                    consecutive_failures=0
                )
                if bot:
                    await bot.send_message(TELEGRAM_CHAT_ID, f"⚠️ <b>Skipped {thread_id}</b> (too many errors)")
            else:
                delay = RETRY_DELAYS[min(consecutive_failures, len(RETRY_DELAYS)-1)]
                next_retry = int(time.time()) + delay
                logger.error(f"Error {thread_id}: {result['message']}")
                self.db.record_bump_failure(thread_id, result['message'], next_retry)
                
        return result

    async def run_cycle(self):
        ready = self.db.get_threads_ready_for_bump()
        if not ready: return
        
        logger.info(f"Ready threads: {len(ready)}")
        results = []
        
        for thread in ready:
            if not self.running: break
            res = await self.process_thread(thread)
            results.append(res)
            await asyncio.sleep(API_DELAY)
            
        if results and bot:
            await self._notify_summary(results)

    async def _notify_summary(self, results: list):
        bumped = [r for r in results if r['success']]
        
        all_threads = self.db.get_all_threads()
        now = int(time.time())
        pending = [
            t for t in all_threads 
            if t.get('is_active') and t['next_bump_time'] > now
        ]
        
        lines = []
        if bumped:
            lines.append(f"<b>Success: {len(bumped)}</b>")
            for item in bumped:
                title = item.get('title', str(item['thread_id']))[:45]
                lines.append(f"<code>{title}</code>")
            lines.append("")
            
        if pending:
            lines.append(f"<b>Pending: {len(pending)}</b>")
            for item in pending:
                title = item.get('title', str(item['thread_id']))[:35]
                wait_sec = max(0, item['next_bump_time'] - now)
                h, m = wait_sec // 3600, (wait_sec % 3600) // 60
                
                wait_str = f"{h}h {m}m" if h > 0 else f"{m}m"
                lines.append(f"<code>{title}</code> - <b>in {wait_str}</b>")
        
        if lines:
            try:
                await bot.send_message(TELEGRAM_CHAT_ID, "\n".join(lines))
            except Exception as e:
                logger.error(f"Telegram error: {e}")

@dp.message(Command("status"))
async def status_handler(message: types.Message):
    if ADMIN_USER_IDS and message.from_user.id not in ADMIN_USER_IDS:
        return

    db = Database()
    threads = db.get_all_threads()
    active = [t for t in threads if t.get('is_active')]
    now = int(time.time())
    
    text = f"<b>System Status</b>\nActive: {len(active)}\n\n"
    for t in active:
        tid = t['thread_id']
        wait = max(0, t['next_bump_time'] - now)
        
        if wait == 0:
            status = "READY"
        else:
            h, m = wait // 3600, (wait % 3600) // 60
            status = f"{h}h {m}m"
            
        text += f"ID: <code>{tid}</code>\n➤ <b>{status}</b>\n\n"
        
    await message.answer(text)

@dp.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("^.^")

async def bump_loop(service):
    logger.info("Bump loop started")
    await service.initialize_threads()
    
    if bot:
        try:
            await bot.send_message(TELEGRAM_CHAT_ID, "<b>Started</b>")
        except: pass
        
    while service.running:
        try:
            await service.run_cycle()
            await asyncio.sleep(CHECK_INTERVAL)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception(f"Loop error: {e}")
            await asyncio.sleep(60)

async def main():
    service = AsyncBumpService()
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    def signal_handler():
        logger.info("Shutdown signal received")
        service.running = False
        stop_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)
    bump_task = asyncio.create_task(bump_loop(service))
    try:
        if bot:
            logger.info("Starting polling + bump loop")
            await asyncio.gather(dp.start_polling(bot), bump_task, return_exceptions=True)
        else:
            logger.info("Starting bump loop only (No Bot)")
            await bump_task
    except asyncio.CancelledError:
        pass
    finally:
        await service.close()
        if bot:
            await bot.session.close()
        logger.info("Goodbye!")
if __name__ == "__main__":
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logger.add("bump.log", rotation="10 MB", retention="30 days", level="DEBUG")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logger.info("Force exit")

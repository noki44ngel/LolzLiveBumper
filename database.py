import sqlite3
import time
from contextlib import contextmanager
from loguru import logger

from config import DATABASE_PATH


class Database:
    def __init__(self, db_path: str = DATABASE_PATH):
        self.db_path = db_path
        self._init_db()
    
    @contextmanager
    def _get_connection(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            conn.close()
    
    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS threads (
                    thread_id INTEGER PRIMARY KEY,
                    title TEXT,
                    last_bump_time INTEGER DEFAULT 0,
                    next_bump_time INTEGER DEFAULT 0,
                    bump_count INTEGER DEFAULT 0,
                    last_error TEXT,
                    consecutive_failures INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    created_at INTEGER,
                    updated_at INTEGER
                )
            ''')
            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bump_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    thread_id INTEGER,
                    bump_time INTEGER,
                    success INTEGER,
                    message TEXT,
                    FOREIGN KEY (thread_id) REFERENCES threads(thread_id)
                )
            ''')
            
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_next_bump ON threads(next_bump_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_history_thread ON bump_history(thread_id)')

            cursor.execute('PRAGMA journal_mode=WAL;')
            
            logger.info(f"Database initialized: {self.db_path} (WAL enabled)")
    
    def get_thread(self, thread_id: int) -> dict | None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM threads WHERE thread_id = ?', (thread_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    
    def upsert_thread(self, thread_id: int, **kwargs):
        now = int(time.time())
        existing = self.get_thread(thread_id)
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            if existing:
                set_parts = []
                values = []
                for key, value in kwargs.items():
                    set_parts.append(f"{key} = ?")
                    values.append(value)
                set_parts.append("updated_at = ?")
                values.append(now)
                values.append(thread_id)
                
                query = f"UPDATE threads SET {', '.join(set_parts)} WHERE thread_id = ?"
                cursor.execute(query, values)
            else:
                kwargs['thread_id'] = thread_id
                kwargs['created_at'] = now
                kwargs['updated_at'] = now
                
                columns = ', '.join(kwargs.keys())
                placeholders = ', '.join(['?' for _ in kwargs])
                query = f"INSERT INTO threads ({columns}) VALUES ({placeholders})"
                cursor.execute(query, list(kwargs.values()))
    
    def get_threads_ready_for_bump(self) -> list[dict]:
        now = int(time.time())
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM threads 
                WHERE is_active = 1 AND next_bump_time <= ?
                ORDER BY next_bump_time ASC
            ''', (now,))
            return [dict(row) for row in cursor.fetchall()]
    
    def get_all_threads(self) -> list[dict]:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM threads ORDER BY thread_id')
            return [dict(row) for row in cursor.fetchall()]
    
    def record_bump_success(self, thread_id: int, message: str, next_bump_time: int):
        now = int(time.time())
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE threads SET 
                    last_bump_time = ?,
                    next_bump_time = ?,
                    bump_count = bump_count + 1,
                    last_error = NULL,
                    consecutive_failures = 0,
                    updated_at = ?
                WHERE thread_id = ?
            ''', (now, next_bump_time, now, thread_id))
            
            cursor.execute('''
                INSERT INTO bump_history (thread_id, bump_time, success, message)
                VALUES (?, ?, 1, ?)
            ''', (thread_id, now, message))
    
    def record_bump_failure(self, thread_id: int, error: str, next_retry_time: int):
        now = int(time.time())
        
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
                UPDATE threads SET 
                    next_bump_time = ?,
                    last_error = ?,
                    consecutive_failures = consecutive_failures + 1,
                    updated_at = ?
                WHERE thread_id = ?
            ''', (next_retry_time, error, now, thread_id))
            
            cursor.execute('''
                INSERT INTO bump_history (thread_id, bump_time, success, message)
                VALUES (?, ?, 0, ?)
            ''', (thread_id, now, error))
    
    def reset_consecutive_failures(self, thread_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE threads SET consecutive_failures = 0, updated_at = ?
                WHERE thread_id = ?
            ''', (int(time.time()), thread_id))
    
    def deactivate_thread(self, thread_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE threads SET is_active = 0, updated_at = ?
                WHERE thread_id = ?
            ''', (int(time.time()), thread_id))
    
    def activate_thread(self, thread_id: int):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE threads SET is_active = 1, consecutive_failures = 0, updated_at = ?
                WHERE thread_id = ?
            ''', (int(time.time()), thread_id))
    
    def get_stats(self) -> dict:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('SELECT COUNT(*) as total, SUM(is_active) as active FROM threads')
            threads = cursor.fetchone()
            
            day_ago = int(time.time()) - 86400
            cursor.execute('''
                SELECT 
                    COUNT(*) as total,
                    SUM(success) as successful,
                    COUNT(*) - SUM(success) as failed
                FROM bump_history 
                WHERE bump_time > ?
            ''', (day_ago,))
            history = cursor.fetchone()
            
            return {
                'total_threads': threads['total'] or 0,
                'active_threads': threads['active'] or 0,
                'bumps_24h': history['total'] or 0,
                'successful_24h': history['successful'] or 0,
                'failed_24h': history['failed'] or 0,
            }
    
    def cleanup_old_history(self, days: int = 30):
        cutoff = int(time.time()) - (days * 86400)
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM bump_history WHERE bump_time < ?', (cutoff,))
            deleted = cursor.rowcount
            if deleted > 0:
                logger.info(f"Cleaned up {deleted} old history records")

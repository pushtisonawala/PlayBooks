import logging

import redis

from executor.source_processors.processor import Processor

logger = logging.getLogger(__name__)

# Commands the connector is allowed to run through the "Run Command" task.
# Restricted to read-only / introspection commands so a playbook can never
# mutate or administer the target Redis instance.
REDIS_READ_ONLY_COMMANDS = {
    'GET', 'MGET', 'STRLEN', 'GETRANGE', 'SUBSTR',
    'EXISTS', 'TYPE', 'TTL', 'PTTL', 'EXPIRETIME', 'PEXPIRETIME',
    'KEYS', 'SCAN', 'RANDOMKEY', 'DBSIZE', 'OBJECT',
    'HGET', 'HMGET', 'HGETALL', 'HKEYS', 'HVALS', 'HLEN', 'HEXISTS', 'HSTRLEN', 'HSCAN',
    'LLEN', 'LRANGE', 'LINDEX', 'LPOS',
    'SCARD', 'SISMEMBER', 'SMISMEMBER', 'SMEMBERS', 'SRANDMEMBER', 'SSCAN', 'SINTER', 'SUNION', 'SDIFF',
    'ZCARD', 'ZCOUNT', 'ZRANGE', 'ZRANGEBYSCORE', 'ZRANGEBYLEX', 'ZRANK', 'ZREVRANK', 'ZSCORE', 'ZMSCORE', 'ZSCAN',
    'XLEN', 'XRANGE', 'XREVRANGE', 'XINFO', 'XPENDING',
    'BITCOUNT', 'BITPOS', 'GETBIT', 'PFCOUNT', 'GEOPOS', 'GEODIST', 'GEOSEARCH',
    'INFO', 'MEMORY', 'CLIENT', 'CONFIG', 'SLOWLOG', 'LATENCY', 'COMMAND',
    'PING', 'ECHO', 'TIME', 'LOLWUT', 'DEBUG', 'LASTSAVE', 'WAIT',
}

# Sub-commands that must be blocked even though their parent command is
# otherwise read-only (e.g. CONFIG SET, MEMORY PURGE, CLIENT KILL).
REDIS_BLOCKED_SUBCOMMANDS = {
    ('CONFIG', 'SET'), ('CONFIG', 'RESETSTAT'), ('CONFIG', 'REWRITE'),
    ('MEMORY', 'PURGE'), ('MEMORY', 'MALLOC-STATS'),
    ('CLIENT', 'KILL'), ('CLIENT', 'SETNAME'), ('CLIENT', 'PAUSE'), ('CLIENT', 'UNPAUSE'),
    ('CLIENT', 'NO-EVICT'), ('CLIENT', 'NO-TOUCH'), ('CLIENT', 'SETINFO'),
    ('SLOWLOG', 'RESET'),
    ('LATENCY', 'RESET'),
    ('DEBUG', 'SLEEP'), ('DEBUG', 'SEGFAULT'), ('DEBUG', 'PANIC'), ('DEBUG', 'QUICKLIST-PACKED-THRESHOLD'),
    ('DEBUG', 'SET-ACTIVE-EXPIRE'), ('DEBUG', 'JMAP'), ('DEBUG', 'CHANGE-REPL-ID'),
}


class RedisCommandNotAllowed(Exception):
    pass


def _decode(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode('utf-8', errors='replace')
    if isinstance(value, (list, tuple)):
        return ' '.join(_decode(v) for v in value)
    return value


class RedisProcessor(Processor):
    client = None

    def __init__(self, host, port=6379, password=None, db=0, ssl_enabled=False):
        self.config = {
            'host': host,
            'port': int(port) if port else 6379,
            'db': int(db) if db else 0,
            'socket_timeout': 15,
            'socket_connect_timeout': 15,
            'decode_responses': True,
        }
        if password:
            self.config['password'] = password
        if str(ssl_enabled).lower() == 'true' or ssl_enabled is True:
            self.config['ssl'] = True

    def get_connection(self):
        try:
            if not self.client:
                self.client = redis.Redis(**self.config)
            return self.client
        except Exception as e:
            logger.error(f"Exception occurred while creating redis connection with error: {e}")
            raise e

    def test_connection(self):
        try:
            client = self.get_connection()
            client.ping()
            return True
        except Exception as e:
            logger.error(f"Exception occurred while testing redis connection with error: {e}")
            raise e

    def get_info(self, section=None):
        try:
            client = self.get_connection()
            if section:
                return client.info(section)
            return client.info()
        except Exception as e:
            logger.error(f"Exception occurred while fetching redis INFO with error: {e}")
            raise e

    def get_slowlog(self, limit=10):
        try:
            client = self.get_connection()
            limit = int(limit) if limit else 10
            entries = client.slowlog_get(limit)
            normalised = []
            for entry in entries:
                normalised.append({
                    'id': _decode(entry.get('id')),
                    'start_time': _decode(entry.get('start_time')),
                    'duration_us': _decode(entry.get('duration')),
                    'command': _decode(entry.get('command')),
                    'client_address': _decode(entry.get('client_address')),
                    'client_name': _decode(entry.get('client_name')),
                })
            return normalised
        except Exception as e:
            logger.error(f"Exception occurred while fetching redis SLOWLOG with error: {e}")
            raise e

    @staticmethod
    def _validate_read_only(tokens):
        if not tokens:
            raise RedisCommandNotAllowed("Empty command")
        command = tokens[0].upper()
        if command not in REDIS_READ_ONLY_COMMANDS:
            raise RedisCommandNotAllowed(
                f"Command '{command}' is not permitted. Only read-only commands can be run through this connector."
            )
        if len(tokens) > 1 and (command, tokens[1].upper()) in REDIS_BLOCKED_SUBCOMMANDS:
            raise RedisCommandNotAllowed(f"Command '{command} {tokens[1].upper()}' is not permitted.")
        return command

    def run_command(self, command_str):
        try:
            tokens = command_str.split()
            self._validate_read_only(tokens)
            client = self.get_connection()
            return client.execute_command(*tokens)
        except RedisCommandNotAllowed:
            raise
        except Exception as e:
            logger.error(f"Exception occurred while running redis command with error: {e}")
            raise e

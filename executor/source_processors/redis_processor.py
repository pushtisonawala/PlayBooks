import logging
import shlex

import redis

from executor.source_processors.processor import Processor

logger = logging.getLogger(__name__)

# Commands the connector is allowed to run through the "Run Command" task,
# with no sub-command to worry about. Restricted to read-only / introspection
# commands so a playbook can never mutate or administer the target instance.
#
# Not included on purpose (and not allow-listed below either):
#   DEBUG   - DEBUG POPULATE writes keys, DEBUG RELOAD/RESTART/CRASH-AND-RECOVER
#             disrupt the server; DEBUG is enabled by default on Redis < 7.
#   WAIT    - blocks the connection until enough replicas ack or the timeout
#             hits; not read-only in any useful sense here.
REDIS_READ_ONLY_COMMANDS = {
    'GET', 'MGET', 'STRLEN', 'GETRANGE', 'SUBSTR',
    'EXISTS', 'TYPE', 'TTL', 'PTTL', 'EXPIRETIME', 'PEXPIRETIME',
    'KEYS', 'SCAN', 'RANDOMKEY', 'DBSIZE',
    'HGET', 'HMGET', 'HGETALL', 'HKEYS', 'HVALS', 'HLEN', 'HEXISTS', 'HSTRLEN', 'HSCAN',
    'LLEN', 'LRANGE', 'LINDEX', 'LPOS',
    'SCARD', 'SISMEMBER', 'SMISMEMBER', 'SMEMBERS', 'SRANDMEMBER', 'SSCAN', 'SINTER', 'SUNION', 'SDIFF',
    'ZCARD', 'ZCOUNT', 'ZRANGE', 'ZRANGEBYSCORE', 'ZRANGEBYLEX', 'ZRANK', 'ZREVRANK', 'ZSCORE', 'ZMSCORE', 'ZSCAN',
    'XLEN', 'XRANGE', 'XREVRANGE', 'XPENDING',
    'BITCOUNT', 'BITPOS', 'GETBIT', 'PFCOUNT', 'GEOPOS', 'GEODIST', 'GEOSEARCH',
    'PING', 'ECHO', 'TIME', 'LOLWUT', 'LASTSAVE',
    'INFO',
}

# Commands that also carry dangerous sub-commands (mutation, admin, or
# connection-disrupting actions) are NOT allow-listed wholesale. Only the
# sub-commands below - which are read-only introspection - are permitted.
# Anything not listed here for these commands (CLIENT KILL/PAUSE/REPLY/
# TRACKING/SETNAME, MEMORY PURGE, CONFIG SET/RESETSTAT/REWRITE, SLOWLOG RESET,
# LATENCY RESET, ...) is rejected.
REDIS_ALLOWED_SUBCOMMANDS = {
    'CLIENT': {'LIST', 'INFO', 'ID', 'GETNAME'},
    'MEMORY': {'USAGE', 'STATS', 'DOCTOR'},
    'SLOWLOG': {'GET', 'LEN'},
    'LATENCY': {'LATEST', 'HISTORY', 'DOCTOR'},
    'OBJECT': {'ENCODING', 'FREQ', 'IDLETIME', 'REFCOUNT'},
    'XINFO': {'STREAM', 'GROUPS', 'CONSUMERS'},
    'COMMAND': {'COUNT', 'DOCS', 'INFO', 'LIST', 'GETKEYS'},
    # CONFIG GET can return secrets (requirepass, masterauth, ...); the
    # result is filtered in run_command() below rather than trusting the
    # request pattern, since a glob like `CONFIG GET *` would otherwise
    # still return them.
    'CONFIG': {'GET'},
}

# Config keys CONFIG GET must never return, plus a substring fallback so a
# future/unknown Redis config option that looks like a credential is caught
# even if it isn't in the explicit set.
_SENSITIVE_CONFIG_KEYS = {
    'requirepass', 'masterauth', 'masteruser', 'aclfile',
    'tls-key-file', 'tls-key-file-pass', 'tls-client-key-file', 'tls-client-key-file-pass',
    'tls-cert-file', 'tls-ca-cert-file', 'tls-ca-cert-dir',
    'dir', 'dbfilename',
}
_SENSITIVE_CONFIG_KEY_MARKERS = ('pass', 'auth', 'secret')


class RedisCommandNotAllowed(Exception):
    pass


def _decode(value):
    if isinstance(value, (bytes, bytearray)):
        return value.decode('utf-8', errors='replace')
    if isinstance(value, (list, tuple)):
        return ' '.join(_decode(v) for v in value)
    return value


def _is_sensitive_config_key(key) -> bool:
    key_lower = str(key).lower()
    if key_lower in _SENSITIVE_CONFIG_KEYS:
        return True
    return any(marker in key_lower for marker in _SENSITIVE_CONFIG_KEY_MARKERS)


def _filter_config_result(result):
    """Strip sensitive keys from a CONFIG GET result, whatever shape the
    redis-py version in use returns it as (dict, or the flat
    [key, value, key, value, ...] list execute_command() gives back)."""
    if isinstance(result, dict):
        return {k: v for k, v in result.items() if not _is_sensitive_config_key(k)}
    if isinstance(result, (list, tuple)):
        filtered = []
        for i in range(0, len(result) - 1, 2):
            key, value = result[i], result[i + 1]
            if not _is_sensitive_config_key(key):
                filtered.extend([key, value])
        return filtered
    return result


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
        if str(ssl_enabled).lower() == 'true':
            self.config['ssl'] = True

    def get_connection(self):
        try:
            if not self.client:
                self.client = redis.Redis(**self.config)
            return self.client
        except Exception as e:
            logger.error(f"Exception occurred while creating redis connection with error: {e}")
            self.client = None
            raise e

    def test_connection(self):
        try:
            client = self.get_connection()
            client.ping()
            return True
        except Exception as e:
            logger.error(f"Exception occurred while testing redis connection with error: {e}")
            self.client = None
            raise e

    def get_info(self, section=None):
        try:
            client = self.get_connection()
            if section:
                return client.info(section)
            return client.info()
        except Exception as e:
            logger.error(f"Exception occurred while fetching redis INFO with error: {e}")
            self.client = None
            raise e

    def get_slowlog(self, limit=10):
        try:
            client = self.get_connection()
            entries = client.slowlog_get(limit)
            normalised = []
            for entry in entries:
                # Even with decode_responses=True, slowlog_get() can still
                # hand back bytes for these fields (verified against the
                # redis==4.6.0 pinned in requirements.txt) - decode explicitly.
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
            self.client = None
            raise e

    @staticmethod
    def _validate_read_only(tokens):
        if not tokens:
            raise RedisCommandNotAllowed("Empty command")
        command = tokens[0].upper()
        subcommand = tokens[1].upper() if len(tokens) > 1 else None

        if command in REDIS_ALLOWED_SUBCOMMANDS:
            if command == 'COMMAND' and subcommand is None:
                return command  # bare COMMAND lists the command table; read-only
            if subcommand not in REDIS_ALLOWED_SUBCOMMANDS[command]:
                allowed = ', '.join(sorted(REDIS_ALLOWED_SUBCOMMANDS[command]))
                attempted = f"{command} {subcommand}" if subcommand else command
                raise RedisCommandNotAllowed(
                    f"'{attempted}' is not permitted. "
                    f"Allowed {command} sub-commands: {allowed}."
                )
            return command

        if command in REDIS_READ_ONLY_COMMANDS:
            return command

        raise RedisCommandNotAllowed(
            f"Command '{command}' is not permitted. Only read-only commands can be run through this connector."
        )

    def run_command(self, command_str):
        try:
            tokens = shlex.split(command_str)
            command = self._validate_read_only(tokens)
            client = self.get_connection()
            result = client.execute_command(*tokens)
            if command == 'CONFIG':
                result = _filter_config_result(result)
            return result
        except RedisCommandNotAllowed:
            raise
        except Exception as e:
            logger.error(f"Exception occurred while running redis command with error: {e}")
            self.client = None
            raise e

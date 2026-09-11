from unittest import mock

from django.test import TestCase

from executor.source_processors.redis_processor import RedisProcessor, RedisCommandNotAllowed


class TestRedisProcessor(TestCase):

    @mock.patch('redis.Redis')
    def test_successful_connection(self, mock_redis):
        mock_client = mock.Mock()
        mock_redis.return_value = mock_client

        processor = RedisProcessor(host='localhost', port=6379, password='pass', db=0)
        connection = processor.get_connection()

        self.assertEqual(connection, mock_client)
        _, kwargs = mock_redis.call_args
        self.assertEqual(kwargs['host'], 'localhost')
        self.assertEqual(kwargs['port'], 6379)
        self.assertEqual(kwargs['password'], 'pass')
        self.assertEqual(kwargs['db'], 0)

    @mock.patch('redis.Redis')
    def test_password_omitted_when_blank(self, mock_redis):
        RedisProcessor(host='localhost', password='').get_connection()
        _, kwargs = mock_redis.call_args
        self.assertNotIn('password', kwargs)

    @mock.patch('redis.Redis')
    def test_ssl_enabled_flag(self, mock_redis):
        RedisProcessor(host='localhost', ssl_enabled='true').get_connection()
        _, kwargs = mock_redis.call_args
        self.assertTrue(kwargs['ssl'])

    @mock.patch('executor.source_processors.redis_processor.logger')
    @mock.patch('redis.Redis', side_effect=Exception("connection refused"))
    def test_connection_failure(self, mock_redis, mock_logger):
        processor = RedisProcessor(host='localhost')
        with self.assertRaises(Exception) as ctx:
            processor.get_connection()
        self.assertEqual(str(ctx.exception), "connection refused")

    @mock.patch('redis.Redis')
    def test_test_connection_pings(self, mock_redis):
        mock_client = mock.Mock()
        mock_redis.return_value = mock_client
        self.assertTrue(RedisProcessor(host='localhost').test_connection())
        mock_client.ping.assert_called_once()

    @mock.patch('redis.Redis')
    def test_get_info_with_section(self, mock_redis):
        mock_client = mock.Mock()
        mock_client.info.return_value = {'used_memory': 1024}
        mock_redis.return_value = mock_client

        result = RedisProcessor(host='localhost').get_info('memory')
        self.assertEqual(result, {'used_memory': 1024})
        mock_client.info.assert_called_once_with('memory')

    @mock.patch('redis.Redis')
    def test_get_slowlog_normalises_entries(self, mock_redis):
        mock_client = mock.Mock()
        mock_client.slowlog_get.return_value = [{
            'id': 1,
            'start_time': 1700000000,
            'duration': 1500,
            'command': [b'GET', b'foo'],
            'client_address': b'127.0.0.1:5000',
            'client_name': b'',
        }]
        mock_redis.return_value = mock_client

        result = RedisProcessor(host='localhost').get_slowlog(5)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['command'], 'GET foo')
        self.assertEqual(result[0]['duration_us'], 1500)
        self.assertEqual(result[0]['client_address'], '127.0.0.1:5000')
        mock_client.slowlog_get.assert_called_once_with(5)

    @mock.patch('redis.Redis')
    def test_run_command_allows_read_only(self, mock_redis):
        mock_client = mock.Mock()
        mock_client.execute_command.return_value = 42
        mock_redis.return_value = mock_client

        result = RedisProcessor(host='localhost').run_command('DBSIZE')
        self.assertEqual(result, 42)
        mock_client.execute_command.assert_called_once_with('DBSIZE')

    def test_run_command_blocks_write_command(self):
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('SET foo bar')

    def test_run_command_blocks_flushall(self):
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('FLUSHALL')

    def test_run_command_blocks_config_set(self):
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('CONFIG SET maxmemory 100mb')

    def test_run_command_allows_config_get(self):
        with mock.patch('redis.Redis') as mock_redis:
            mock_client = mock.Mock()
            mock_client.execute_command.return_value = ['maxmemory', '0']
            mock_redis.return_value = mock_client
            result = RedisProcessor(host='localhost').run_command('CONFIG GET maxmemory')
            self.assertEqual(result, ['maxmemory', '0'])

    def test_run_command_config_get_filters_password(self):
        # CONFIG GET requirepass must never surface the password, even though
        # GET is the allowed sub-command - the result is filtered, not the request.
        with mock.patch('redis.Redis') as mock_redis:
            mock_client = mock.Mock()
            mock_client.execute_command.return_value = ['requirepass', 'super-secret']
            mock_redis.return_value = mock_client
            result = RedisProcessor(host='localhost').run_command('CONFIG GET requirepass')
            self.assertEqual(result, [])

    def test_run_command_config_get_wildcard_filters_password_only(self):
        with mock.patch('redis.Redis') as mock_redis:
            mock_client = mock.Mock()
            mock_client.execute_command.return_value = [
                'maxmemory', '0', 'requirepass', 'super-secret', 'masterauth', 'also-secret',
            ]
            mock_redis.return_value = mock_client
            result = RedisProcessor(host='localhost').run_command('CONFIG GET *')
            self.assertEqual(result, ['maxmemory', '0'])

    def test_run_command_config_get_filters_dict_shape(self):
        # Some redis-py versions/paths return CONFIG GET as a dict rather than
        # a flat list - filtering must handle both.
        with mock.patch('redis.Redis') as mock_redis:
            mock_client = mock.Mock()
            mock_client.execute_command.return_value = {'requirepass': 'secret', 'maxmemory': '0'}
            mock_redis.return_value = mock_client
            result = RedisProcessor(host='localhost').run_command('CONFIG GET *')
            self.assertEqual(result, {'maxmemory': '0'})

    def test_run_command_blocks_debug(self):
        # DEBUG POPULATE/RELOAD/RESTART are destructive/disruptive; DEBUG is
        # not allow-listed at all rather than trying to enumerate every
        # dangerous sub-command.
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('DEBUG POPULATE 1000')

    def test_run_command_blocks_wait(self):
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('WAIT 1 0')

    def test_run_command_blocks_client_reply(self):
        # CLIENT REPLY OFF would also wedge the cached connection for every
        # later command on this processor instance.
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('CLIENT REPLY OFF')

    def test_run_command_blocks_client_kill(self):
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('CLIENT KILL ID 1')

    def test_run_command_allows_client_list(self):
        with mock.patch('redis.Redis') as mock_redis:
            mock_client = mock.Mock()
            mock_client.execute_command.return_value = 'id=1 addr=127.0.0.1:0'
            mock_redis.return_value = mock_client
            result = RedisProcessor(host='localhost').run_command('CLIENT LIST')
            self.assertEqual(result, 'id=1 addr=127.0.0.1:0')

    def test_run_command_blocks_memory_purge(self):
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('MEMORY PURGE')

    def test_run_command_allows_memory_usage(self):
        with mock.patch('redis.Redis') as mock_redis:
            mock_client = mock.Mock()
            mock_client.execute_command.return_value = 56
            mock_redis.return_value = mock_client
            result = RedisProcessor(host='localhost').run_command('MEMORY USAGE mykey')
            self.assertEqual(result, 56)

    def test_run_command_blocks_slowlog_reset(self):
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('SLOWLOG RESET')

    def test_run_command_allows_bare_command(self):
        # Bare COMMAND (no sub-command) lists the command table - read-only.
        with mock.patch('redis.Redis') as mock_redis:
            mock_client = mock.Mock()
            mock_client.execute_command.return_value = []
            mock_redis.return_value = mock_client
            RedisProcessor(host='localhost').run_command('COMMAND')
            mock_client.execute_command.assert_called_once_with('COMMAND')

    def test_run_command_blocks_command_getkeysandflags(self):
        with self.assertRaises(RedisCommandNotAllowed):
            RedisProcessor(host='localhost').run_command('COMMAND GETKEYSANDFLAGS SET foo bar')

    def test_run_command_handles_quoted_argument(self):
        with mock.patch('redis.Redis') as mock_redis:
            mock_client = mock.Mock()
            mock_client.execute_command.return_value = 'bar'
            mock_redis.return_value = mock_client
            RedisProcessor(host='localhost').run_command('GET "my key"')
            mock_client.execute_command.assert_called_once_with('GET', 'my key')

    def test_run_command_resets_client_on_failure(self):
        # A broken/blocked connection must not be reused by the next call.
        with mock.patch('redis.Redis') as mock_redis:
            mock_client = mock.Mock()
            mock_client.execute_command.side_effect = TimeoutError("timed out")
            mock_redis.return_value = mock_client
            processor = RedisProcessor(host='localhost')
            with self.assertRaises(TimeoutError):
                processor.run_command('DBSIZE')
            self.assertIsNone(processor.client)

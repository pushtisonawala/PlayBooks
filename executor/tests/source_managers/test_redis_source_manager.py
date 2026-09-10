from unittest import mock, TestCase
from unittest.mock import MagicMock

import django

django.setup()

from executor.source_managers.redis_source_manager import RedisSourceManager
from protos.base_pb2 import TimeRange
from protos.playbooks.playbook_commons_pb2 import PlaybookTaskResultType
from protos.playbooks.source_task_definitions.redis_task_pb2 import Redis
from google.protobuf.wrappers_pb2 import StringValue, UInt64Value


class TestRedisSourceManager(TestCase):

    @mock.patch('executor.source_managers.redis_source_manager.generate_credentials_dict')
    @mock.patch('executor.source_managers.redis_source_manager.RedisProcessor')
    def test_get_connector_processor(self, MockRedisProcessor, mock_generate_credentials_dict):
        mock_generate_credentials_dict.return_value = {'host': 'localhost', 'port': '6379'}
        manager = RedisSourceManager()
        connector = MagicMock()
        connector.type = 'redis'
        connector.keys = {}

        processor = manager.get_connector_processor(connector)
        MockRedisProcessor.assert_called_once_with(host='localhost', port='6379')
        self.assertEqual(processor, MockRedisProcessor.return_value)

    @mock.patch('executor.source_managers.redis_source_manager.generate_credentials_dict')
    @mock.patch('executor.source_managers.redis_source_manager.RedisProcessor')
    def test_execute_get_info_returns_table(self, MockRedisProcessor, mock_generate_credentials_dict):
        mock_generate_credentials_dict.return_value = {'host': 'localhost'}
        MockRedisProcessor.return_value.get_info.return_value = {
            'used_memory_human': '1.2M', 'connected_clients': 3,
        }
        manager = RedisSourceManager()
        connector = MagicMock()
        connector.account_id = UInt64Value(value=1)

        task = Redis(type=Redis.TaskType.GET_INFO, get_info=Redis.GetInfo(section=StringValue(value='memory')))
        result = manager.execute_get_info(TimeRange(), task, connector)

        self.assertEqual(result.type, PlaybookTaskResultType.TABLE)
        self.assertEqual(len(result.table.rows), 2)
        MockRedisProcessor.return_value.get_info.assert_called_once_with('memory')

    @mock.patch('executor.source_managers.redis_source_manager.generate_credentials_dict')
    @mock.patch('executor.source_managers.redis_source_manager.RedisProcessor')
    def test_execute_get_slowlog_defaults_limit(self, MockRedisProcessor, mock_generate_credentials_dict):
        mock_generate_credentials_dict.return_value = {'host': 'localhost'}
        MockRedisProcessor.return_value.get_slowlog.return_value = [
            {'id': 1, 'command': 'GET foo', 'duration_us': 12},
        ]
        manager = RedisSourceManager()
        connector = MagicMock()
        connector.account_id = UInt64Value(value=1)

        task = Redis(type=Redis.TaskType.GET_SLOWLOG, get_slowlog=Redis.GetSlowlog())
        result = manager.execute_get_slowlog(TimeRange(), task, connector)

        self.assertEqual(result.type, PlaybookTaskResultType.TABLE)
        MockRedisProcessor.return_value.get_slowlog.assert_called_once_with(10)

    @mock.patch('executor.source_managers.redis_source_manager.generate_credentials_dict')
    @mock.patch('executor.source_managers.redis_source_manager.RedisProcessor')
    def test_execute_run_command(self, MockRedisProcessor, mock_generate_credentials_dict):
        mock_generate_credentials_dict.return_value = {'host': 'localhost'}
        MockRedisProcessor.return_value.run_command.return_value = 5
        manager = RedisSourceManager()
        connector = MagicMock()
        connector.account_id = UInt64Value(value=1)

        task = Redis(type=Redis.TaskType.RUN_COMMAND,
                     run_command=Redis.RunCommand(command=StringValue(value='DBSIZE')))
        result = manager.execute_run_command(TimeRange(), task, connector)

        self.assertEqual(result.type, PlaybookTaskResultType.TABLE)
        self.assertEqual(result.table.rows[0].columns[0].value.value, '5')
        MockRedisProcessor.return_value.run_command.assert_called_once_with('DBSIZE')

    def test_task_type_callable_map_complete(self):
        manager = RedisSourceManager()
        self.assertEqual(
            set(manager.task_type_callable_map.keys()),
            {Redis.TaskType.GET_INFO, Redis.TaskType.GET_SLOWLOG, Redis.TaskType.RUN_COMMAND},
        )
        for info in manager.task_type_callable_map.values():
            self.assertIn('executor', info)
            self.assertIn('display_name', info)
            self.assertEqual(info['result_type'], PlaybookTaskResultType.TABLE)

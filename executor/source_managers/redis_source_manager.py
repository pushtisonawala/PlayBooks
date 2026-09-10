from google.protobuf.wrappers_pb2 import StringValue, UInt64Value, Int64Value

from connectors.utils import generate_credentials_dict
from executor.playbook_source_manager import PlaybookSourceManager
from executor.source_processors.redis_processor import RedisProcessor
from protos.base_pb2 import Source, TimeRange, SourceModelType
from protos.connectors.connector_pb2 import Connector as ConnectorProto
from protos.literal_pb2 import LiteralType, Literal
from protos.playbooks.playbook_commons_pb2 import PlaybookTaskResult, TableResult, PlaybookTaskResultType
from protos.playbooks.source_task_definitions.redis_task_pb2 import Redis
from protos.ui_definition_pb2 import FormField, FormFieldType


class RedisSourceManager(PlaybookSourceManager):

    def __init__(self):
        self.source = Source.REDIS
        self.task_proto = Redis
        self.task_type_callable_map = {
            Redis.TaskType.GET_INFO: {
                'executor': self.execute_get_info,
                'model_types': [SourceModelType.REDIS_CONNECTION],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Fetch Redis INFO stats',
                'category': 'Database',
                'form_fields': [
                    FormField(key_name=StringValue(value="section"),
                              display_name=StringValue(value="INFO section"),
                              description=StringValue(value='e.g. memory, clients, stats, replication. Leave blank for all.'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Redis.TaskType.GET_SLOWLOG: {
                'executor': self.execute_get_slowlog,
                'model_types': [SourceModelType.REDIS_CONNECTION],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Fetch Redis slow query log',
                'category': 'Database',
                'form_fields': [
                    FormField(key_name=StringValue(value="limit"),
                              display_name=StringValue(value="Number of entries"),
                              description=StringValue(value='How many slowlog entries to fetch'),
                              data_type=LiteralType.LONG,
                              default_value=Literal(type=LiteralType.LONG, long=Int64Value(value=10)),
                              form_field_type=FormFieldType.TEXT_FT,
                              is_optional=True),
                ]
            },
            Redis.TaskType.RUN_COMMAND: {
                'executor': self.execute_run_command,
                'model_types': [SourceModelType.REDIS_CONNECTION],
                'result_type': PlaybookTaskResultType.TABLE,
                'display_name': 'Run a read-only Redis command',
                'category': 'Database',
                'form_fields': [
                    FormField(key_name=StringValue(value="command"),
                              display_name=StringValue(value="Command"),
                              description=StringValue(value='Read-only command only, e.g. "DBSIZE", "GET key", "MEMORY USAGE key"'),
                              data_type=LiteralType.STRING,
                              form_field_type=FormFieldType.MULTILINE_FT),
                ]
            },
        }

    def get_connector_processor(self, redis_connector, **kwargs):
        generated_credentials = generate_credentials_dict(redis_connector.type, redis_connector.keys)
        return RedisProcessor(**generated_credentials)

    @staticmethod
    def _table_from_rows(raw_query, rows):
        table_rows = []
        for row in rows:
            columns = [
                TableResult.TableColumn(name=StringValue(value=str(key)), value=StringValue(value=str(value)))
                for key, value in row.items()
            ]
            table_rows.append(TableResult.TableRow(columns=columns))
        return TableResult(
            raw_query=StringValue(value=raw_query),
            total_count=UInt64Value(value=len(table_rows)),
            rows=table_rows,
        )

    def execute_get_info(self, time_range: TimeRange, redis_task: Redis,
                         redis_connector: ConnectorProto) -> PlaybookTaskResult:
        try:
            if not redis_connector:
                raise Exception("Task execution Failed:: No Redis source found")
            section = redis_task.get_info.section.value.strip() if redis_task.get_info.section.value else None
            processor = self.get_connector_processor(redis_connector)

            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Section -> {}".format(
                "Redis", redis_connector.account_id.value, section or 'all'), flush=True)

            info = processor.get_info(section)
            rows = [{'metric': k, 'value': v} for k, v in info.items()]
            table = self._table_from_rows(f'INFO {section}' if section else 'INFO', rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Redis task: {e}")

    def execute_get_slowlog(self, time_range: TimeRange, redis_task: Redis,
                            redis_connector: ConnectorProto) -> PlaybookTaskResult:
        try:
            if not redis_connector:
                raise Exception("Task execution Failed:: No Redis source found")
            limit = redis_task.get_slowlog.limit.value if redis_task.get_slowlog.limit.value else 10
            processor = self.get_connector_processor(redis_connector)

            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Limit -> {}".format(
                "Redis", redis_connector.account_id.value, limit), flush=True)

            entries = processor.get_slowlog(limit)
            table = self._table_from_rows(f'SLOWLOG GET {limit}', entries)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Redis task: {e}")

    def execute_run_command(self, time_range: TimeRange, redis_task: Redis,
                            redis_connector: ConnectorProto) -> PlaybookTaskResult:
        try:
            if not redis_connector:
                raise Exception("Task execution Failed:: No Redis source found")
            command = redis_task.run_command.command.value.strip()
            if not command:
                raise Exception("Task execution Failed:: No command provided")
            processor = self.get_connector_processor(redis_connector)

            print("Playbook Task Downstream Request: Type -> {}, Account -> {}, Command -> {}".format(
                "Redis", redis_connector.account_id.value, command), flush=True)

            result = processor.run_command(command)
            rows = self._normalise_command_result(result)
            table = self._table_from_rows(command, rows)
            return PlaybookTaskResult(type=PlaybookTaskResultType.TABLE, table=table, source=self.source)
        except Exception as e:
            raise Exception(f"Error while executing Redis task: {e}")

    @staticmethod
    def _normalise_command_result(result):
        if isinstance(result, dict):
            return [{'key': k, 'value': v} for k, v in result.items()]
        if isinstance(result, (list, tuple, set)):
            return [{'index': i, 'value': v} for i, v in enumerate(result)]
        return [{'result': result}]

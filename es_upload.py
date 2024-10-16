import datetime
import os
import re
from es_logs.constants import PROFILER_TIME_FMT, STAT_LOG_PATTERN, MAIN_LOOP_EVENT_LOG_PATTERN, \
    MAIN_EVENT_LOG_PATTERN, CLIENT_TRAFFIC_FILE, SERVER_LOG_PATTERN, MSIAB_LOG_PATTERN, LOG_TYPES_MAPPING, NULL_TS, \
    SERVER_LOG_TYPE, SERVER_STATS_LOG_TYPE, SERVER_EVENT_LOG_TYPE, CLIENT_SERVER_ACTIONS_LOG_TYPE, CLIENT_LOG_TYPE, \
    EVENT_LOG_TYPE, BULK_CHUNK_SIZE, HEATMAP_LOG_TYPE, INDEX_REFRESH_INTERVAL, LOADING_TIMES_EVENTS, \
    UI_BENCH_EVENT_LOG, RALT_KEY_EVENT, ES_TIME_FMT, MSIAB_TIME_FMT, MSIAB_LOG_TYPE
from wowslib.utils.file_helpers import get_files_by_regex, copy_file
from es_logs.configs.elasticsearch import ES
from es_logs.es_parsers import PARSERS
from autocore.lazylama.logger import logger
from autocore.lazylama.autostep import autostep
from es_logs.constants import INDEX_SETTINGS
from es_logs.log_combine import shift_log_time, aggregate_log, _parse_dates
from es_logs.utils import clean_string
from elasticsearch import TransportError
from elasticsearch.helpers import streaming_bulk


def _get_first_and_last_lines(log):
    with open(log, 'rb') as f:
        first_line = f.readline()
        f.seek(-2, os.SEEK_END)
        while f.read(1) != b"\n":
            f.seek(-2, os.SEEK_CUR)
        last_line = f.readline()
    return [first_line, last_line]


def _get_last_time_stamp(log):
    last_line = _get_first_and_last_lines(log)[1]
    return _parse_dates(last_line.decode("utf-8").split(',')[0])


class ESUploader:
    def __init__(self, index, alias, tags, directory, upload_all_events, test_counter):
        self.index = index
        self.alias = clean_string(alias) if alias else None
        self.tags = tags
        self.mapping = self.create_mapping()
        self.es = ES().connection()
        self.directory = directory
        self.profiler_logs = get_files_by_regex(self.directory, STAT_LOG_PATTERN)
        self.traffic_logs = get_files_by_regex(self.directory, SERVER_LOG_PATTERN)
        if (self.directory / CLIENT_TRAFFIC_FILE).exists():
            self.traffic_logs.append(self.directory / CLIENT_TRAFFIC_FILE)
        self.main_loop_event_logs = get_files_by_regex(self.directory, MAIN_LOOP_EVENT_LOG_PATTERN)
        self.event_logs = get_files_by_regex(self.directory, MAIN_EVENT_LOG_PATTERN)
        self.msiab_logs = get_files_by_regex(self.directory, MSIAB_LOG_PATTERN)
        self.traffic_logs = get_files_by_regex(self.directory, SERVER_LOG_PATTERN)
        last_ts = _get_last_time_stamp(self.profiler_logs[0]) if self.profiler_logs else NULL_TS
        self.log_end_time = last_ts if last_ts else NULL_TS
        self.log_start_time = datetime.timedelta()
        self.upload_all_events = upload_all_events
        self.ui_bench_event_log = None
        self.test_counter = test_counter

    @autostep
    def shift_logs_to(self, shift_to):
        self.log_start_time = shift_log_time(self.profiler_logs, self.main_loop_event_logs, shift_to)
        return f'Logs shifted to {shift_to}'

    @autostep
    def aggregate_logs(self):
        for file in self.profiler_logs:
            aggregate_log(file)
        return 'Logs aggregated'

    def create_mapping(self):
        field_definition = {'type': 'keyword'}
        keywords = {'test_start': field_definition, 'title': field_definition}
        if self.tags:
            for tag in self.tags:
                keywords.update({tag: field_definition})
        keywords.update({'type': {'type': 'text', "fields": {
            "keyword": {
                "type": "keyword"
            }
        }}})
        mapping = {
            'properties': {
                'Time': {'type': 'date', 'format': PROFILER_TIME_FMT},
            }
        }
        mapping['properties'].update(keywords)
        return mapping

    def create_es_index(self):
        try:
            self.es.indices.create(self.index, INDEX_SETTINGS)
        except TransportError:
            logger.info(f'Index "{self.index}" already exist. Skipping index creation')

    def update_es_mapping(self):
        try:
            self.es.indices.put_mapping(body=self.mapping, index=self.index)
        except TransportError as e:
            logger.info(e)

    @autostep
    def prepare_index(self):
        logger.info(f'Creating index {self.index}')
        self.create_es_index()
        logger.info('Updating index mapping')
        self.update_es_mapping()
        if self.alias:
            logger.info(f'Updating index alias to "{self.alias}"')
            self.es.indices.update_aliases(body={'actions': [{'add': {'index': self.index, 'alias': self.alias}}]})
        return f'Index {self.index} with alias {self.alias} prepared'

    def _update_tags(self, log, log_pattern):
        logger.info(f'Extracting timestamp from log name "{log}"')
        search = log_pattern.search(log.name)
        assert search, 'Failed to parse timestamp from log'
        self.tags.update({'test_start': search.group(1)})

    def _parse_log_to_es(self, log, log_type, **kwargs):
        tags = {key: clean_string(value) for key, value in self.tags.items()}
        encoding = 'cp1252' if log_type == MSIAB_LOG_TYPE else 'utf-8'
        if log_type in [SERVER_LOG_TYPE, SERVER_STATS_LOG_TYPE]:
            total_lines = len(log.index)
        elif log_type in [SERVER_EVENT_LOG_TYPE, CLIENT_SERVER_ACTIONS_LOG_TYPE]:
            total_lines = len(log)
        else:
            with open(log, 'r', encoding=encoding) as f:
                total_lines = sum(1 for _ in f) - 1
        progress = 0
        log_parser = PARSERS.get(log_type, PARSERS.get(CLIENT_LOG_TYPE))

        for ok, response in streaming_bulk(self.es, log_parser(self.index, log, tags, **kwargs),
                                           chunk_size=BULK_CHUNK_SIZE,
                                           max_retries=5,
                                           request_timeout=100):
            progress += 1
            percent = 100 if log_type in [EVENT_LOG_TYPE, HEATMAP_LOG_TYPE] else (float(progress) / total_lines) * 100
            if progress % BULK_CHUNK_SIZE == 0 or percent == 100:
                logger.info(f'Progress {progress} / {total_lines}. {percent:.1f} %')

    def prepare_event_logs(self):
        for main_loop_log, log in zip(self.main_loop_event_logs, self.event_logs):
            logger.info('Merge and clean event logs')
            copy_file(main_loop_log, main_loop_log.parent / main_loop_log.name.replace('thread.', 'thread_original.'))
            copy_file(log, log.parent / log.name.replace('thread.', 'thread_original.'))

            with open(main_loop_log, 'a') as log1, open(log, 'r') as log2:
                log1.write('\n')
                for line in log2:
                    log1.write(line)

            main_loop_log_tmp = main_loop_log.parent / (main_loop_log.name + 'tmp')
            main_loop_log.rename(main_loop_log_tmp)
            events = []
            for elem in LOADING_TIMES_EVENTS:
                events.extend(elem[1])
            with open(main_loop_log_tmp, 'r') as input_log, open(main_loop_log, 'w') as output_log:
                for line in input_log:
                    try:
                        ts, event = line.strip().rstrip(';').split(',')
                    except ValueError:
                        continue
                    if event in events or 'UIBench_' in event or 'ShipyardBench_' in event or (
                            self.log_start_time and self.log_start_time in event):
                        logger.info(f'Clean event log line: {line}')
                        output_log.write(line)
            main_loop_log_tmp.unlink()

    @autostep
    def prepare_ui_event_logs(self):
        self.ui_bench_event_log = self.directory / UI_BENCH_EVENT_LOG
        uiLogs = []
        RAlt_time = None
        for log in self.main_loop_event_logs:
            with open(self.ui_bench_event_log, 'w+') as file1, open(log, 'r') as file2:
                for line in file2:
                    if RAlt_time:
                        uiLogs.append(line.strip().split(','))
                        continue
                    if RALT_KEY_EVENT in line:
                        RAlt_time = datetime.strptime(line.strip().split(',')[0], "%H:%M:%S.%f")
                for event in uiLogs:
                    if 'UIBench' in event[1]:
                        event[0] = str(datetime.strptime(event[0], "%H:%M:%S.%f") - RAlt_time)
                    file1.write((','.join(event) + '\n'))
        return f'{self.ui_bench_event_log} prepared'

    def _upload_log(self, log, log_pattern, test_counter=None):
        logger.info(f'Inserting docs into ElasticSearch with tags {self.tags}')
        log_type = LOG_TYPES_MAPPING.get(log_pattern.pattern)
        self._parse_log_to_es(log, log_type, test_counter=self.test_counter, log_end_time=self.log_end_time,
                              log_start_time=self.log_start_time, upload_all=self.upload_all_events)

    @autostep
    def upload_profiler_logs(self):
        assert self.profiler_logs, 'Required stat log files not found'
        for log in self.profiler_logs:
            self._update_tags(log, STAT_LOG_PATTERN)
            self._upload_log(log, STAT_LOG_PATTERN)
        return f'Logs {self.profiler_logs} uploaded'

    @autostep
    def upload_msiab_logs(self):
        if not self.profiler_logs:
            first_line = _get_first_and_last_lines(self.msiab_logs[0])[0]
            test_start = first_line.decode("utf-8").split(',')[1].strip()
            test_start_formatted = datetime.datetime.strptime(test_start, MSIAB_TIME_FMT).strftime(ES_TIME_FMT)
            self.tags.update({'test_start': test_start_formatted})
            self.tags['title'] = f"{self.tags['title']}_{test_start_formatted}"
        for log in self.msiab_logs:
            self._upload_log(log, MSIAB_LOG_PATTERN)
        return f'Logs {self.msiab_logs} uploaded'

    @autostep
    def upload_events_logs(self):
        assert self.event_logs, 'Required event log files not found'
        for log in self.main_loop_event_logs:
            self._update_tags(log, MAIN_LOOP_EVENT_LOG_PATTERN)
            self._upload_log(log, MAIN_LOOP_EVENT_LOG_PATTERN)
        return f'Logs {self.main_loop_event_logs} uploaded'

    @autostep
    def upload_traffic_logs(self):
        assert self.traffic_logs, 'Required traffic log files not found'
        for log in self.traffic_logs:
            self._update_tags(log, SERVER_LOG_PATTERN)
            self._upload_log(log, SERVER_LOG_PATTERN)
        return f'Logs {self.traffic_logs} uploaded'

    def after_uploading(self):
        logger.info(f'Change refresh_interval in indexes "{self.index}" onto {INDEX_REFRESH_INTERVAL}')
        index_update_settings = {
            "index": {
                "refresh_interval": INDEX_REFRESH_INTERVAL
            }
        }
        self.es.indices.put_settings(body=index_update_settings, index=[self.index])
        for artifact_file in self.directory.glob('*'):
            if re.match(STAT_LOG_PATTERN, artifact_file.name):
                artifact_file.unlink()

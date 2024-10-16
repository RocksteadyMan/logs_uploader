import os
import time
import csv
import datetime
import json
from pathlib import Path
import pandas as pd
from numpy import mean
from typing import List, Tuple, Iterator, Optional
from wowslib.utils.file_helpers import copy_file
from autocore.lazylama.logger import logger
from wowslib.constants.mongo import Collections
from es_logs.configs.mongo import MongoConfig
from es_logs.constants import LOG_THREADS_PATTERN, PERFANA_TIME_FMT, NULL_TS, NUMBER_OF_HEATMAP_CAMERA_TURNS, \
    HEATMAP_DEFAULT_WIDTH, HEATMAP_DEFAULT_INDENT, CLIENT_TRAFFIC_FILE, AGGREGATE_FREQUENCY, AGGREGATE_PERCENTILE


def _convert(value: str) -> float:
    return float(value) if value else 0.0


def _parse_dates(date: str) -> Optional[datetime.datetime]:
    try:
        return datetime.datetime.strptime(date, PERFANA_TIME_FMT)
    except ValueError:
        return None


def _add(thread_data: Iterator, main_frame_data: List[map], prev_frame_ts: datetime.datetime,
         frame_ts: datetime.datetime, last_frame_data: Tuple) -> Tuple[list, Tuple]:
    # thread_data -- генератор с данными из текущего треда
    # main_frame_data -- данные из мейн треда + данные из уже пройденных на данный момент тредов
    # prev_frame_ts -- время начала кадра, время из предшествующей строки
    # frame_ts -- время конца кадра, время из текущей строки
    # last_frame_data -- неучтённые данные из предыдущей итерации для текущего кадра именно для этого треда

    # если таймстемп неучтенных данных для этого треда попадает в текущий кадр или равен нулю (для первого кадра)

    if prev_frame_ts < last_frame_data[0] <= frame_ts or last_frame_data[0] == NULL_TS:
        if len(list(main_frame_data)) == len(list(last_frame_data[1])):
            main_frame_data = [x + y for x, y in zip(main_frame_data, last_frame_data[1])]

        for ts, row in thread_data:
            if ts > frame_ts:
                return main_frame_data, (ts, row)
            if prev_frame_ts < ts <= frame_ts:
                main_frame_data = [x + y for x, y in zip(main_frame_data, row)]
    else:
        pass
    return main_frame_data, last_frame_data


def _get_time_delta(log, event, first_occurrence=False):
    with open(log, 'r') as f:
        event_found = False
        for line in f:
            if event in line:
                ts, stamp = line.split(',')
                event_found = True
                event_time = _parse_dates(ts)
                if first_occurrence:
                    break
    if event_found:
        return datetime.timedelta(minutes=event_time.minute,
                                  seconds=event_time.second,
                                  microseconds=event_time.microsecond)
    else:
        return datetime.timedelta(minutes=0, seconds=0, microseconds=0)


def shift_log_time(profiler_logs, main_loop_event_logs, shift_to):
    log_time_start = None
    for i, log in enumerate(profiler_logs):
        event_log = main_loop_event_logs[i]
        shifted_event_log = log.with_name(log.name.rstrip('.log') + '_shifted.log')
        log_time_start = _get_time_delta(event_log, shift_to) if 'LoadingBattle' not in shift_to else _get_time_delta(
            event_log, shift_to, first_occurrence=True)
        with open(log, 'r') as f, open(shifted_event_log, 'w+') as new_file:
            new_file.write(next(f))
            for line in f:
                temp_line = line.split(',')
                if _parse_dates(temp_line[0]) >= NULL_TS + log_time_start:
                    temp_line[0] = (_parse_dates(temp_line[0]) - log_time_start).strftime(PERFANA_TIME_FMT)[:-3]
                    new_file.write(','.join(temp_line))
        log.rename(log.with_name('tmp'))
        shifted_event_log.rename(log)
        log.with_name('tmp').unlink()
    return log_time_start


def aggregate_log(log, percentile=AGGREGATE_PERCENTILE, freq=AGGREGATE_FREQUENCY):
    logger.info(f'Aggregate log {log}')
    i = pd.DataFrame([])
    for chunk in pd.read_csv(log, chunksize=300, sep=',', date_parser=_parse_dates, index_col='Time'):
        i = i.append(chunk.groupby(pd.Grouper(freq=freq)).quantile(percentile))
    i.reset_index(level=['Time'], inplace=True)
    i['Time'] = i['Time'].apply(lambda x: f"{x.strftime(PERFANA_TIME_FMT)}"[:-3])
    i.set_index('Time', inplace=True)
    i.to_csv(log)


class CombineLogs:
    def __init__(self, folder: Path, start_time: time, files: List[Path] = None):
        self.folder = folder
        self.start_time = start_time
        if files:
            self.files = files
            self._main_threads = files[0]
        else:
            self._main_threads = self._get_threads()
        self._combined_logs = []

    @property
    def main_threads(self):
        return self._main_threads

    @property
    def combined_logs(self):
        return self._combined_logs

    def _search_main_threads(self) -> List[Path]:
        logger.info('Trying to find stat log for main thread...')
        main_treads = [f for f in self.files if 'main_thread' in f.name.lower()]
        if main_treads:
            return main_treads
        else:
            raise AssertionError('Failed to found stat log for main thread')

    def _get_threads(self):
        logger.info(f'Trying to find thread stat logs in {self.folder}...')
        files = [f for f in self.folder.glob('*')
                 if LOG_THREADS_PATTERN.search(f.name) and 'events.log' not in f.name and 'ETW' not in f.name]
        if self.start_time:
            files = [f for f in files if os.stat(f).st_mtime > self.start_time]
        all_files = "\n".join([f.name for f in files])
        logger.info(f'Found stat log files:\n{all_files}')
        self.files = files
        return self._search_main_threads()

    def _search_threads(self, timestamp: str) -> List[Path]:
        thread_files = [f for f in self.files if
                        timestamp in f.name and 'main_thread' not in f.name.lower() and 'events' not in f.name]
        return thread_files

    def _parse_threads(self, thread_file: Path, main_metrics: List[str]):
        time_fields_count = 3
        logger.info(f'Parsing {thread_file.name}...')
        start_parse_threads = time.time()
        with open(thread_file) as thread:
            thread_reader = csv.reader(thread)
            thread_metrics = next(thread_reader)[:-1]
            if main_metrics != thread_metrics:
                raise AssertionError(f'Thread metrics don\'t match main thread in {thread_file.name}')
            for row in thread_reader:
                if not any(row[time_fields_count:-1]):
                    continue
                try:
                    ts = _parse_dates(row[0])
                    row_data = list(map(_convert, row[1:-1]))
                    if sum(row_data) > 0:
                        yield ts, row_data
                except ValueError as e:
                    logger.exception(e)
        logger.info(f'PARSE THREADS TIME: {time.time() - start_parse_threads}')

    def _combine_logs(self, main_thread: Path):
        logger.info('Processing main thread log...')
        start_combine_logs = time.time()

        log_time = LOG_THREADS_PATTERN.search(main_thread.name).group(1)
        has_corrupted_threads = False

        if log_time:
            with open(main_thread, 'rt') as csvfile:
                stats_reader = csv.reader(csvfile)
                metrics = next(stats_reader)[:-1]

                thread_files = self._search_threads(log_time)
                threads = []  # list for generators

                for thread_file in thread_files:
                    threads.append(self._parse_threads(thread_file, metrics))

                start_frame_ts = datetime.datetime.utcfromtimestamp(0)

                logger.info('Start log combine...')
                output_file = main_thread.parent / f'stat_{log_time}.log'

                with open(output_file, 'w') as output:
                    output.write(','.join(metrics) + '\n')
                    # массив для неучтённых данных из предыдущей итерации для текущего кадра для всех тредов
                    unrecorded_data = [(NULL_TS, []) for _ in threads]
                    start_write_output_data = time.time()
                    for line in stats_reader:
                        try:
                            ts_string = line[0]
                            frame_ts = _parse_dates(ts_string)
                            frame_data = list(map(_convert, line[1:-1]))

                            for i, thread in enumerate(threads):
                                try:
                                    frame_data, unrecorded_data[i] = _add(thread, frame_data, start_frame_ts,
                                                                          frame_ts, unrecorded_data[i])
                                except AssertionError as e:
                                    has_corrupted_threads = True
                                    logger.exception(e)

                            start_frame_ts = frame_ts
                            combined_log_line = ','.join([ts_string] + list(map(str, frame_data))) + '\n'
                            output.write(combined_log_line)
                        except ValueError as e:
                            logger.exception(e)
                    end_write_output_data = time.time()
                    logger.info(f'WRITE OUTPUT DATA TIME: {end_write_output_data - start_write_output_data}')
                self._combined_logs.append(output_file)
            assert not has_corrupted_threads, 'Some threads failed'
            logger.info('Logs successfully combined')
        else:
            raise AssertionError('Failed to extract time from file name')
        end_combine_logs = time.time()
        logger.info(f'COMBINE LOGS TIME: {end_combine_logs - start_combine_logs}')

    def threads_combine(self):
        for main_thread in self.main_threads:
            logger.info(f'Found stat log for main thread: {main_thread.name}')
            self._combine_logs(main_thread)

    def copy_main_thread(self):
        for main_thread in self.main_threads:
            copy_file(main_thread, main_thread.parent / main_thread.name.replace('_thread_Main_thread', ''))

    def combine_heatmap_logs(self):
        stat_combined_file = self.combined_logs[0]
        assert stat_combined_file, 'Combined stat log not found'
        logger.info(f'Combine heatmap logs from {stat_combined_file.name}...')
        event_file = stat_combined_file.parent / stat_combined_file.name.replace('.log',
                                                                                 '_thread_Main_thread.events.log')
        steps = int(os.getenv('GRID_STEP'))
        assert steps, '\'GRID_STEP\' not found in envs'
        map_name = os.getenv('MAP')
        assert map_name, '\'MAP\' not found in envs'
        current_start = 0
        current_end = 0
        intervals_at_points = []
        camera_move_wait = datetime.timedelta(seconds=1)
        with open(event_file) as events:
            for line_num, line in enumerate(events):
                if 'maptest' in line:
                    if current_start == 0:
                        current_start = current_end = line.split(',')[0]
                    else:
                        current_start = current_end
                        sec_before_current_end = datetime.datetime.strptime(line.split(',')[0],
                                                                            PERFANA_TIME_FMT) - camera_move_wait
                        intervals_at_points.append(
                            [current_start, sec_before_current_end.strftime(PERFANA_TIME_FMT)[:-3]])
        with open(stat_combined_file) as stats:
            count = 0
            metrics_names = stats.readline().split(',')[1:]
            line = stats.readline()
            while count < steps * steps * NUMBER_OF_HEATMAP_CAMERA_TURNS and line:
                data = line.split(',')
                ts, metrics = data[0], data[1:]
                if intervals_at_points[count][1] > ts > intervals_at_points[count][0]:
                    if len(intervals_at_points[count]) == 2:
                        for metric in metrics:
                            intervals_at_points[count].append([float(metric)])
                    else:
                        for id, metric in enumerate(metrics, start=1):
                            intervals_at_points[count][id + 1].append(float(metric))

                elif ts > intervals_at_points[count][1]:
                    count += 1
                line = stats.readline()

        heatmap_metrics = {'StatThread': []}
        for id, metric in enumerate(metrics_names):
            means_by_points = []
            for point in intervals_at_points:
                means_by_points.append(mean(point[id + 2]))
            heatmap_metrics['StatThread'].append({'Name': metric, 'Id': id, 'Budget': mean(means_by_points)})

        for point in intervals_at_points:
            for metric in range(len(point) - 2):
                point[metric + 2] = [mean(point[metric + 2]), max(point[metric + 2])]

        ordered_points = []
        for point_x in range(steps):
            for point_y in range(steps):
                ordered_points = ordered_points + intervals_at_points[8 * (point_x * steps + point_y): 8 * (point_x * steps + point_y) + 3]
            for point_y in range(steps):
                ordered_points.append(intervals_at_points[8 * (point_x * steps + point_y) + 7])
                ordered_points.append(intervals_at_points[8 * (point_x * steps + point_y) + 3])
            for point_y in range(steps):
                ordered_points.append(intervals_at_points[8 * (point_x * steps + point_y) + 6])
                ordered_points.append(intervals_at_points[8 * (point_x * steps + point_y) + 5])
                ordered_points.append(intervals_at_points[8 * (point_x * steps + point_y) + 4])

        heatmap_data = []
        for point in ordered_points:
            heatmap_data.append({'Data': point[2:]})
        heatmap_file_name = stat_combined_file.parent / stat_combined_file.name.replace('.log', '_heatmap.log')
        minimaps_collection = MongoConfig().collection(Collections.MINIMAPS)

        map_info = minimaps_collection.find_one({'mapName': map_name})
        assert map_info, f'No data for map {map_name} found in mongo'
        margin = map_info.get('mapIndent', HEATMAP_DEFAULT_INDENT)
        width = map_info.get('mapWidth', HEATMAP_DEFAULT_WIDTH)
        start_point = margin + width / (2 * steps)
        minimap_start = {'X': start_point, 'Y': start_point}
        minimap_end = {'X': width - start_point, 'Y': width - start_point}

        with open(heatmap_file_name, 'w') as fp:
            json.dump(
                {'heatmap_data': heatmap_data, 'heatmap_metrics': heatmap_metrics, 'minimap_start': minimap_start,
                 'minimap_end': minimap_end}, fp)
        logger.info(f'Heatmaps combined in {heatmap_file_name.name}')

    def aggregate_client_traffic(self):
        logger.info('Aggregate client traffic')
        main_thread_file = self._combined_logs[0]
        df = pd.read_csv(main_thread_file, usecols=['Traffic_Downloaded', 'Traffic_Uploaded', 'Time'], sep=',',
                         date_parser=_parse_dates, index_col='Time')
        i = df.resample('1s').sum()
        i.reset_index(level=['Time'], inplace=True)
        i['Time'] = i['Time'].apply(lambda x: f"{x.strftime(PERFANA_TIME_FMT)}"[:-3])
        i.set_index('Time', inplace=True)
        i.columns = ['Traffic_Uploaded_Bytes_per_sec', 'Traffic_Downloaded_Bytes_per_sec']
        traffic_file_name = main_thread_file.with_name(CLIENT_TRAFFIC_FILE)
        i.to_csv(traffic_file_name)
        return traffic_file_name

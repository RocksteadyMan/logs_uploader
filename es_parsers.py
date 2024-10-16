import re
import csv
import datetime
from itertools import count
from es_logs.constants import TIME_PATTERN, STAT_LOG_PATTERN, NULL_TS, CLIENT_LOG_TYPE, MSIAB_LOG_TYPE, \
    EVENT_LOG_TYPE, ES_TIME_FMT, MSIAB_TIME_FMT, PERFANA_TIME_FMT, perfana_ts, LOADING_TIMES_EVENTS, EVENT_TIME_GAP, \
    METRIC_COUNTERS, RALT_KEY_EVENT, EVENTS_PREFIXES
from autocore.lazylama.logger import logger
from wowslib.utils.file_helpers import get_files_by_regex


def _get_difference(value_1, value_2):
    if value_1 is None or value_2 is None:
        return None
    return float(value_1) - float(value_2) if float(value_2) >= 0 and float(value_1) >= 0 and float(value_1) >= float(
        value_2) else None


def _parse_stat_log(index, log_file, tags, **kwargs):
    with open(log_file, 'r') as log:
        header = next(log).strip()
        metrics = header.split(',')[1:]
        for line in log:
            message = {}
            values = line.strip().split(',')
            metrics_values = values[1:]
            time_stamp = values[0]
            if len(metrics) != len(metrics_values):
                logger.info('Metrics list length is not equal to metrics values in string')
                continue
            message.update(tags)
            message.update({'type': CLIENT_LOG_TYPE})
            message.update({'Time': time_stamp})
            for metric, value in zip(metrics, metrics_values):
                try:
                    if value and value != '0.000000':
                        message.update({metric: float(value)})
                except ValueError:
                    logger.info(f'Failed to convert: {metric} = {value}')
            yield {
                "_index": index,
                "_source": message
            }


def _parse_msiab_log(index, log_file, tags, **kwargs):
    log_start_time = kwargs.get('log_start_time')
    log_end_time = kwargs.get('log_end_time')
    with open(log_file, 'r', encoding='cp1252') as log:
        line = next(log)
        while line[0:2] != '02':
            line = next(log)
        metric_names = re.sub(' +', ' ', line).split(', ')[2].split(' ,')
        metrics = []
        for name in metric_names:
            if 'Memory usage' in name or 'RAM usage' in name or 'Commit charge' in name:
                measure = 'Mb'
            elif 'clock' in name:
                measure = 'MHz'
            elif 'temperature' in name:
                measure = 'C'
            else:
                measure = r'%'
            metrics.append(f'HW_{name.replace(" ", "_")}({measure})')
        test_start_time = datetime.datetime.strptime(tags['test_start'], ES_TIME_FMT)
        if log_start_time == datetime.timedelta() and log_end_time == NULL_TS:
            start_time = test_start_time
            end_time = datetime.datetime.max
        else:
            start_time = test_start_time + log_start_time - datetime.timedelta(microseconds=log_start_time.microseconds)
            end_time = test_start_time + datetime.timedelta(hours=log_end_time.hour, seconds=log_end_time.second,
                                                            minutes=log_end_time.minute)
        while datetime.datetime.strptime(next(log).split(', ')[1], MSIAB_TIME_FMT) != start_time:
            next(log)
        for pos, line in enumerate(log):
            message = {}
            metrics_values = line.replace(',', '').split()[3:]
            cur_time = NULL_TS + datetime.timedelta(milliseconds=pos * 100)
            line_ts = datetime.datetime.strptime(line.split(', ')[1], MSIAB_TIME_FMT)
            if line_ts > end_time:
                break
            time_stamp = cur_time.strftime(PERFANA_TIME_FMT)[:-3]
            message.update(tags)
            message.update({'type': CLIENT_LOG_TYPE})
            message.update({'Time': time_stamp})
            for metric, value in zip(metrics, metrics_values):
                try:
                    if value and value != '0.000':
                        message.update({metric: float(value)})
                except ValueError:
                    if not metric.startswith('HW_Frame'):
                        logger.info(f'Failed to convert: {metric} = {value}')
            yield {
                "_index": index,
                "_source": message
            }


def _collect_counters(event_log, start_time):
    counter_stats = {}
    stat_logs = get_files_by_regex(event_log.parent, STAT_LOG_PATTERN)
    stat_log = [log for log in stat_logs if start_time in log.name][0]
    with open(stat_log, 'r') as log:
        reader = csv.DictReader(log)
        for row in reader:
            stat_ts = row.get('Time')
            if stat_ts:
                counter_stats[stat_ts] = tuple([stat_ts] + [row.get(counter, None) for counter in METRIC_COUNTERS])
    return counter_stats


def _calc_events_stats(log_file, start_time, upload_all):
    counter_stats = _collect_counters(log_file, start_time)
    with open(log_file, 'r') as log:
        event_stats = {'AppStart_0': (NULL_TS, 'AppStart') + (0,) * len(next(iter(counter_stats.values())))}
        counter_stats_times = sorted(counter_stats.keys())
        for line in log:
            try:
                ts, events = line.strip().rstrip(';').split(',')
            except ValueError:
                continue
            if events:
                added_line = counter_stats.get(ts)
                if not added_line:
                    previous_counter_stats_times = filter(lambda counter_stats_ts: counter_stats_ts <= ts,
                                                          counter_stats_times)
                    *_, previous = previous_counter_stats_times
                    added_line = counter_stats[previous]
                    if '' in added_line:
                        added_line = tuple(item or '0' for item in added_line)
                logger.info(f'Added line: {added_line}')
                for event in events.split(';'):
                    if not event_stats.get(f'{event}_0'):
                        event_stats.update(
                            {f'{event}_0': (perfana_ts(ts), f'{event}_0') + added_line[1:]})
                    elif upload_all:
                        count = 1
                        while event_stats.get(f'{event}_{count}'):
                            count += 1
                        if list(event_stats.items())[-1][0] != f'{event}_{count - 1}':
                            event_stats.update(
                                {f'{event}_{count}': (perfana_ts(ts), f'{event}_{count}') + added_line[1:]})
        logger.info(f'Log events stats:\n {event_stats}')
        return event_stats


def _generate_messages(event, event_stats, bounds, test_count):
    start_event_bound = bounds[0]
    end_event_bound = bounds[1]
    for num in count():
        message = {}
        start_event = event_stats.get(f'{start_event_bound}_{num}')
        end_event = event_stats.get(f'{end_event_bound}_{num}')
        if not (start_event and end_event):
            break
        logger.info(f'start_event {start_event_bound}')
        logger.info(f'end_event {bounds[1]}')
        start_event_time = start_event[0]
        end_event_time = end_event[0]
        if start_event and end_event and end_event_time >= start_event_time:
            message[event] = round((end_event_time - start_event_time).total_seconds(), 1)
            start_event_time += datetime.timedelta(minutes=int(test_count), seconds=int(EVENT_TIME_GAP * num))
            message.update({'Time': start_event_time.strftime(PERFANA_TIME_FMT)[:-3]})
            for index, counter in enumerate(METRIC_COUNTERS):
                message[f'{counter}_{event}'] = _get_difference(end_event[index + 2], start_event[index + 2])
            yield message
        else:
            logger.warning('Check start_event or end_event')


def _gen_ui_action_message(index, tags, events_info):
    r_alt_time = NULL_TS
    message = dict(tags, type='events')
    events = {}
    for event, info in events_info.items():
        if event.startswith(RALT_KEY_EVENT):
            r_alt_time = info[0]
        for events_prefix in EVENTS_PREFIXES:
            if not event.startswith(events_prefix):
                continue
            ts = (info[0] - r_alt_time + NULL_TS).strftime(PERFANA_TIME_FMT)[:-3]
            events.update({event: ts})
    message['events'] = events
    return {"_index": index, "_source": message}


def _parse_event_log(index, log_file, tags, **kwargs):
    upload_all = kwargs.get('upload_all', False)
    test_count = int(kwargs.get('test_counter')) if kwargs.get('test_counter') else 0
    tags.update({'type': EVENT_LOG_TYPE})
    start_time = re.search(TIME_PATTERN, log_file.name).group(1)
    event_stats = _calc_events_stats(log_file, start_time, upload_all)
    for event, bounds in LOADING_TIMES_EVENTS:
        messages = _generate_messages(event, event_stats, bounds, test_count)
        for message in messages:
            result = tags.copy()
            result.update(message)
            yield {
                "_index": index,
                "_source": result
            }
    yield _gen_ui_action_message(index, tags, event_stats)


PARSERS = {
    CLIENT_LOG_TYPE: _parse_stat_log,
    MSIAB_LOG_TYPE: _parse_msiab_log,
    EVENT_LOG_TYPE: _parse_event_log,
}

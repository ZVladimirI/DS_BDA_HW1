from datetime import datetime
from os import path
import os
import argparse
import random
import shutil


def generate_malformed_row():
    return str(datetime.now()) + 'malformed'


def generate_devices(num):
    return {i: 'dev_' + str(i) for i in range(num)}


def generate_log_str(dev, stamp):
    return str(dev)+','+str(stamp*1000)+','+str(random.random()*100)+'\n'


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Log geneartor')
    parser.add_argument('--start-date', type=str,
                        help='Start date and time for log file like dd.mm.YYYY/HH:MM:SS')
    parser.add_argument('--end-date', type=str,
                        help='End date for log file dd.mm.YYYY/HH:MM:SS')
    parser.add_argument('--period', type=int,
                        help='Seconds between records', default=10)
    parser.add_argument('--devices', type=int,
                        help='Number of devices for metrics', default=5)
    parser.add_argument('--output-dir', type=str,
                        help='Output directory for generated files', default='./data')
    parser.add_argument('--mapping-file', type=str,
                        help='Number of devices for metrics', default='mapping')
    parser.add_argument('--num-files', type=int,
                        help='Number of devices for metrics', default=1)
    parser.add_argument('--malformed', type=float,
                        help='Percent of malformed rows', default=1.0)

    options = parser.parse_args()

    start = int(datetime.strptime(options.start_date.replace('/', ' ')+',0', '%d.%m.%Y %H:%M:%S,%f').timestamp())
    end = int(datetime.strptime(options.end_date.replace('/', ' ')+',0', '%d.%m.%Y %H:%M:%S,%f').timestamp())

    assert end > start

    try:
        shutil.rmtree(path.abspath(options.output_dir))
    except FileNotFoundError:
        pass

    os.mkdir(path.abspath(options.output_dir))
    os.mkdir(path.abspath(path.join(options.output_dir, 'input')))

    interval = end - start
    num_iterations = interval // options.period
    batch_size = num_iterations // options.num_files
    borders = list()
    borders.append(start)
    for i in range(options.num_files-1):
        borders.append(borders[i]+batch_size*options.period)
    borders.append(end)

    devices = generate_devices(options.devices)

    with open(path.abspath(path.join(options.output_dir, options.mapping_file)), 'w+') as f:
        for dev in devices:
            f.write(str(dev)+','+devices[dev]+'\n')

    for i in range(options.num_files):
        with open(path.abspath(path.join(options.output_dir, 'input', str(i))), 'w+') as f:
            for stamp in range(borders[i], borders[i+1]):
                for dev in devices:
                    if random.random() < options.malformed / 100:
                        if random.random() < 0.5:
                            dev = dev * options.devices * 10
                        else:
                            f.write(generate_malformed_row())
                            continue
                    f.write(generate_log_str(dev, stamp))

#!/usr/bin/env python3

"""
Aggregation test data generator

Generates random XML and CSV files with a following structure:

* XML:
    ```
    <?xml version="1.0" encoding="UTF-8"?>
    <root>
        <row>
            <start_page>321</start_page>
            <referer>4432</referer>
            <user>5</user>
            <ts>2016-11-02 16:37:03.240</ts>
            <depth>223</depth>
            <duration>543</duration>
            <transmit>7</transmit>
            <type>2345</type>
        </row>
    </root>
    ```

* CSV:
    ```
    start_page,referer,user,ts,depth,duration,transmit,type
    321,4432,5,2016-11-02 16:37:03.240,223,543,7,2345
    ```
"""


import datetime
import logging
import random
import time
import xml.etree.cElementTree as ET


log = logging.getLogger(__name__)


class RandomData:
    """ Defines randomized data string with a given name and limits
    """
    def __init__(self, name, type='int', min=1, max=1000000000):
        """ Creates RandomData object

        :name: str - data name
        :type: 'int'|'ts' - type
        :min: int|ts - minimum limit
        :max: int|ts - maximum limit
        """
        self.name = name
        self.min = min
        self.max = max
        self.value = RandomData.rand_value(type, min, max)

    def __repl__(self):
        return dict([(self.name, self.value)])

    def __str__(self):
        return str(self.value)

    @staticmethod
    def rand_value(value_type, min, max):
        """ Generates random value by given type """
        def rand_int(min, max):
            """ Returns random integer in given limits """
            return random.randrange(min, max)

        def rand_ts(min, max):
            """ Returns random timestamp in given limits """
            def str_to_ts(ts):
                return datetime.datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')

            min = str_to_ts(min)
            max = str_to_ts(max)

            # Delta of two timestamps in seconds
            delta = (max - min).total_seconds()
            # Randomized delta in range(0, delta)
            delta_rand = random.uniform(0, delta)

            return min + datetime.timedelta(0, delta_rand)

        # Returns strategy
        if value_type == 'int':
            return rand_int(min, max)
        elif value_type == 'ts':
            return rand_ts(min, max)


class DataString:
    """ RandomData container
    """
    def __init__(self):
        self.string = (
            RandomData('start_page'),
            RandomData('user', 'int', 1, 100000000),
            RandomData('ts', 'ts', '2005-01-01 23:56:42', '2016-01-01 10:50:00'),
            RandomData('depth', 'int', 1, 50),
            RandomData('duration', 'int', 100, 10000),
            RandomData('transmit', 'int', 100000, 1000000000),
            RandomData('type', 'int', 1, 5)
        )

    def headers(self):
        """ Returns joined RandomData names """
        return ','.join([data.name for data in self.string])

    def values(self):
        """ Returns joined RandomData values """
        return ','.join([str(data.value) for data in self.string])

    def __repr__(self):
        return {data.name: data.value for data in self.string}
    
    def __str__(self):
        return str(self.__repr__())


def measure_time(func):
    """ Function duration measure """
    def wrapper(*args, **kwargs):
        start_time = time.time()
        log.info('Now runs: %s', func)
        log.info('Started at: %s', time.ctime(start_time))

        func(*args, **kwargs)

        end_time = time.time()
        log.info('Ended at: %s', time.ctime(end_time))
        log.info('Duration: %s', end_time - start_time)
    return wrapper


@measure_time
def generate_csv(file, data_rows):
    """ Generate CSV files """
    strings_to_output = data_rows

    with open(file, 'w') as output:
        output.write(DataString().headers())

        for _ in range(1, strings_to_output):
            output.write('\n' + DataString().values())


@measure_time
def generate_xml(file, data_rows):
    """ Generate XML files """
    strings_to_output = data_rows

    root = ET.Element('root')
    
    for _ in range(1, strings_to_output):
        row = ET.SubElement(root, 'row')
        for node in DataString().string:
            ET.SubElement(row, node.name).text = str(node.value)

    tree = ET.ElementTree(root)
    tree.write(file)

    # ET cannot(?) write DTD
    with open(file, 'r') as original:
        data = original.read()
    with open(file, 'w') as modified:
        modified.write('<?xml version="1.0" encoding="UTF-8"?>\n' + data)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)

    log.info('Start')
    
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(2) as pool:
        pool.submit(generate_csv, 'example_data_log.csv', 200000)
        pool.submit(generate_xml, 'example_data_log.xml', 50000)

    log.info('Done')

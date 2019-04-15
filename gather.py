import sys
import json
import os
from yahoo_historical import Fetcher
import pandas as pd
import argparse
from datetime import datetime


class Data:
    def __init__(self, config):
        self.master_path = os.path.abspath(config['path'])
        self.symbols = self.read_symbols(
            config['symbols']['name'], config['symbols']['separator'])
        self.format = config['date_range']['format'].replace(
            'mm', '%m').replace('dd', '%d').replace('yyyy', '%Y')
        self.today = datetime.today()
        self.start_date = datetime.strptime(
            config['date_range']['start'], self.format)
        self.start_yahoo = [
            int(i) for i in self.start_date.strftime('%Y/%m/%d').split('/')]

        self.end_date = datetime.strptime(
            config['date_range']['end'], self.format) if config['date_range']['end'] != 'current' else self.today
        self.end_yahoo = [int(i)
                          for i in self.end_date.strftime('%Y/%m/%d').split('/')]

    def complete_data_check(self, path, movement):
        if movement:
            meta_path = os.path.join(path, 'current_range.txt')
            if not os.path.isfile(meta_path):
                with open(meta_path, 'w') as meta:
                    meta.write(self.start_date.strftime(self.format) +
                               '-' + self.end_date.strftime(self.format))
                return True
            else:
                with open(meta_path, 'r') as meta:
                    range = meta.readline().split('-')
                saved_start = datetime.strptime(range[0], self.format)
                saved_end = datetime.strptime(range[1], self.format)

                if saved_end < self.today or saved_start > self.start_date:
                    return True
                else:
                    return False

    def download_market(self):
        full_path = os.path.join(self.master_path, 'Movements')
        if not os.path.exists(full_path):
            os.makedirs(full_path)
        missing = self.complete_data_check(full_path, True)

        if missing:
            def get_fetcher():
                try:
                    fetch = Fetcher(
                        self.symbols.iloc[0]['Symbol'], self.start_yahoo, self.end_yahoo)
                    return fetch
                except KeyError:
                    get_fetcher()
            fetcher = get_fetcher()
            for index, row in self.symbols.iterrows():
                os.system('clear')
                print('Downloaded symbols: %s' % (str(index) + '/' + str(self.symbols['Symbol'].count())))
                fetcher.ticker = row['Symbol']
                data = fetcher.getHistorical()
                data.to_csv(os.path.join(full_path, row['Symbol']) + '.csv')

    def read_symbols(self, filename, sep):
        df = pd.read_csv(os.path.join(self.master_path, filename), sep=sep)
        return df

    def prepare(self):
        self.download_market()


def read_config(name):
    with open('config.json', 'r') as json_config:
        config = json.load(json_config)
    return config


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-news', nargs='?')
    # args = parser.parse_args()
    config = read_config('config.json')
    data = Data(config)
    data.prepare()


if __name__ == "__main__":
    main()

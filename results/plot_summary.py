# Copyright (c) Facebook, Inc. and its affiliates.
from re import findall, search, split
import matplotlib.pyplot as plt
from matplotlib.ticker import StrMethodFormatter
from glob import glob
from itertools import cycle
import matplotlib.ticker as ticker


# FuncFormatter can be used as a decorator
@ticker.FuncFormatter
def major_formatter(x, pos):
    return f"{x/1000:0.0f}k"

def major_formatter_sec(x, pos):
    return f"{x/1000:0.1f}"

class PlotError(Exception):
    pass

markers = cycle(['o', 'v', 's', 'd'])
styles = cycle(['solid', 'dashed', 'dashdot', 'dotted'])

def find_max_x_less_than_y(x_values, y_values, cutoff_y):
    return max([(x, y) for (x,y) in zip(x_values, y_values) if y < cutoff_y])


class Ploter:
    def __init__(self, filenames):
        if not filenames:
            raise PlotError('No data to plot')

        self.results = []
        try:
            for filename in filenames:
                with open(filename, 'r') as f:
                    self.results += [f.read().replace(',', '')]
        except OSError as e:
            raise PlotError(f'Failed to load log files: {e}')

    def _natural_keys(self, text):
        def try_cast(text): return int(text) if text.isdigit() else text
        return [try_cast(c) for c in split('(\d+)', text)]

    def _tps(self, data):
        values = findall(r' TPS: (\d+) \+/- (\d+)', data)
        values = [(int(x), int(y)) for x, y in values]
        return list(zip(*values))

    def _latency(self, data, scale=1):
        values = findall(r' Latency: (\d+) \+/- (\d+)', data)
        values = [(float(x)/scale, float(y)/scale) for x, y in values]
        return list(zip(*values))

    def _variable(self, data):
        return [int(x) for x in findall(r'Variable value: X=(\d+)', data)]

    def _nodes(self, data, system):
        x = search(r'Committee size: (\d+)', data).group(1)
        f = search(r'Faults: (\d+)', data).group(1)
        faults = f'({f} faulty)' if f != '0' else ''
        return (x, f'{system} - {x} validators {faults}')

    def plot_latency(self, name, new=True, latency_cutoff=4_500):
        if new:
            plt.figure(figsize=[5, 3])

        style = next(styles)
        self.results.sort(key=self._natural_keys)
        for result in self.results:
            y_values, y_err = self._latency(result)
            x_values = self._variable(result)
            if len(y_values) != len(y_err) or len(y_err) != len(x_values):
                raise PlotError('Unequal number of x, y, and y_err values')

            try:
                (val_num, label) = self._nodes(result, name)
                if int(val_num) != 20:
                    continue
            except:
                continue
            if int(val_num) <= 4:
                continue

            try:
                (best_x, best_y) = find_max_x_less_than_y(x_values, y_values, latency_cutoff)
            except:
                print(f"WARNING: Skip {name} ")
                continue

            tick = '*y'
            pt = 12
            if name in ["Batched-HotStuff", "Baseline-HotStuff"]:
                tick = 'ok'
                pt = 5

            print(name, (best_x, best_y))
            plt.plot([best_x], [best_y], tick, markersize=pt, alpha=0.90)
            plt.annotate(f'{name}-{val_num}', (best_x, best_y), xytext=(0, -15), textcoords='offset points', arrowprops={'arrowstyle': '-', 'alpha':0.3}, alpha=0.75, fontsize='small')


        # plt.legend(loc='lower center', bbox_to_anchor=(0.5, 1), ncol=3)
        # plt.xscale('log',base=2)
        # plt.xlim(left=500, right=180_000)
        plt.ylim(bottom=0, top=4_500)
        plt.xlabel('Throughput (tx/s)')
        plt.ylabel('Latency (s)')
        plt.grid(True, which='both')
        ax = plt.gca()
        ax.xaxis.set_major_formatter(major_formatter)
        ax.yaxis.set_major_formatter(major_formatter_sec)

if __name__ == '__main__':
    new = True
    names = ['Narwhal-HotStuff', 'Tusk', 'Batched-HotStuff', 'Baseline-HotStuff']
    for system, name in zip(['HS-on-Dag', 'Tusk', 'Batched HS', 'Baseline HS'], names):
        files = glob(f'data/{system}/aggregated/committee/*.txt')
        print(system, files)
        plot = Ploter(files).plot_latency(name, new)
        new = False

    hs_workers = open("data/HS-on-Dag/aggregated/scaling/agg-4-x-0-512-1000-any-4000.txt").read()
    workers = findall("Variable value: X=(\d+)", hs_workers)
    tps = findall("TPS: (\d+)", hs_workers)
    latency = findall("Latency: (\d+)", hs_workers)

    name = 'Narwhal-HotStuff'
    for (w,t,l) in zip(workers, tps, latency):
        w,t,l = int(w), int(t), int(l)
        if w != 10:
            continue
        val_num = f'4W{w}'
        plt.plot([t], [l], '+r', markersize=12, alpha=0.90)
        plt.annotate(f'{name}-{val_num}', (t, l), xytext=(0, -15), textcoords='offset points', arrowprops={'arrowstyle': '-', 'alpha':0.3}, alpha=0.75, ha='right', fontsize='small')

    hs_workers = open("data/Tusk/aggregated/scaling/agg-4-x-0-512-1000-any-4000.txt").read()
    workers = findall("Variable value: X=(\d+)", hs_workers)
    tps = findall("TPS: (\d+)", hs_workers)
    latency = findall("Latency: (\d+)", hs_workers)

    name = 'Tusk'
    for (w,t,l) in zip(workers, tps, latency):
        w,t,l = int(w), int(t), int(l)
        if w != 10:
            continue
        val_num = f'4W{w}'
        plt.plot([t], [l], '+r', markersize=12, alpha=0.90)
        plt.annotate(f'{name}-{val_num}', (t, l), xytext=(0, -10), textcoords='offset points', arrowprops={'arrowstyle': '-', 'alpha':0.3}, alpha=0.75, ha='right', fontsize='small')

    for x in ['pdf', 'png']:
        plt.savefig(f'figures/latency_summary.{x}', bbox_inches='tight')

from collections import namedtuple
import csv
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


class Plot(object):
    def __init__(self):
        self.record = None
        self.data = list(self.read())
        self.frame()

    def read(self):
        with open('tests/tcpperf_connections.csv', 'r') as f:
            reader = csv.reader(f)
            self.record = namedtuple('record', [k.strip().replace('# ', '')
                                                for k in next(reader)])
            for row_raw in reader:
                row = self.record._make([int(v) for v in row_raw])
                yield row

    def frame(self):
        fig, ax = plt.subplots()

        x = [row.messages for row in self.data]

        # add some text for labels, title and axes ticks
        ax.set_xlabel('connections per second')
        ax.set_ylabel('processed messages per second')
        # ax.set_xticks(x)
        ax.set_xlim(-300, max(x) + 300)
        ax.set_ylim(-300, 6000 + 300)

        fig.tight_layout()

        self.fig, self.ax = fig, ax
        return self

    def plot(self):
        x = [row.messages for row in self.data]

        ideal, = self.ax.plot(x, x, label='ideal', color='black',
                              linestyle='--', linewidth=1)
        graphs = [
            self.ax.plot(x, [getattr(row, k) for row in self.data], label=k)
            for k in self.record._fields if k != 'messages'
        ]

        self.ax.legend(
            handles=[ideal] + [g for g, in graphs],
            loc='upper left',
        )

        return self

    def show(self):
        plt.show()
        return self

    def save(self):
        self.fig.savefig('tests/tcpperf_plot.pdf')
        self.fig.savefig('tests/tcpperf_plot.png', dpi=300)
        return self


if __name__ == '__main__':
    Plot().plot().save()

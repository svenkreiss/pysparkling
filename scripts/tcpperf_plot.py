from collections import namedtuple
import csv

import matplotlib
import matplotlib.pyplot as plt

matplotlib.use('Agg')


class Plot:
    def __init__(self, filename, x_label=None, y_label=None):
        self.filename = filename
        self.x_label = x_label or 'connections per second'
        self.y_label = y_label or 'processed messages per second'
        self.record = None
        self.data = list(self.read())
        self.frame()

    def read(self):
        with open(self.filename, 'r', encoding='utf8') as f:
            reader = csv.reader(f)

            try:
                first_line = next(reader)
            except StopIteration:
                return

            self.record = namedtuple('record', [k.strip().replace('# ', '')
                                                for k in first_line])
            for row_raw in reader:
                row = self.record._make([int(v) for v in row_raw])
                yield row

    def frame(self):
        fig, ax = plt.subplots()

        x = [row.messages for row in self.data]
        y = [row.hello for row in self.data]

        # add some text for labels, title and axes ticks
        ax.set_xlabel(self.x_label)
        ax.set_ylabel(self.y_label)
        # ax.set_xticks(x)
        ax.set_xlim(-300, max(x) + 300)
        ax.set_ylim(-300, max(y) + 2000)

        fig.tight_layout()

        self.fig, self.ax = fig, ax
        return self

    def plot(self):
        x = [row.messages for row in self.data]

        ideal, = self.ax.plot([0.0, max(x)], [0.0, max(x)], label='ideal',
                              color='black', linestyle='--', linewidth=1)
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
        self.fig.savefig(self.filename + '.pdf')
        self.fig.savefig(self.filename + '.png', dpi=300)
        return self


if __name__ == '__main__':
    Plot('tests/tcpperf_connections.csv').plot().save()
    (Plot('tests/tcpperf_messages.csv',
          x_label='inbound messages per second')
     .plot()
     .save())

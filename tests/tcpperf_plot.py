import csv
import matplotlib.pyplot as plt


class Plot(object):
    def __init__(self):
        self.data = list(self.read())
        self.frame()

    def read(self):
        with open('tests/tcpperf_connections.csv', 'r') as f:
            reader = csv.reader(f)
            head = next(reader)
            for row in reader:
                yield {k.strip().replace('# ', ''): int(v)
                       for k, v in zip(head, row)}

    def frame(self):
        fig, ax = plt.subplots()

        x = [row['messages'] for row in self.data]

        # add some text for labels, title and axes ticks
        ax.set_xlabel('emitted messages per second')
        ax.set_ylabel('received messages per second')
        ax.set_xticks(x)
        ax.set_xlim(-300, max(x) + 300)
        ax.set_ylim(-300, 5000 + 300)

        fig.tight_layout()

        self.fig, self.ax = fig, ax
        return self

    def plot(self):
        x = [row['messages'] for row in self.data]

        ideal, = self.ax.plot(x, x, label='ideal', linestyle='--', linewidth=1)
        graphs = [
            self.ax.plot(x, [row[k] for row in self.data], label=k)
            for k in self.data[0].keys() if k != 'messages'
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

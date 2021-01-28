import matplotlib.pyplot as plt
import numpy as np
import test_multiprocessing


def plot(has_hyperthreading=True):
    n_cpu, r = test_multiprocessing.test_performance()
    r = {n: 1.0 / (v[0] / r[1][0]) for n, v in r.items()}

    if has_hyperthreading:
        n_cpu /= 2

    x, y = zip(*sorted(r.items()))
    x_left = np.array(x) - 0.5

    fig, ax = plt.subplots()

    # ideal line
    # line = ax.plot((1, n_cpu), (1.0, n_cpu),
    #                linewidth=2, linestyle='dashed', color='grey')
    # ax.plot((n_cpu, max(x)+0.5), (n_cpu, n_cpu),
    #         linewidth=2, linestyle='dashed', color='grey')
    n_threads = n_cpu * 2 if has_hyperthreading else n_cpu
    bars_ideal = ax.bar(
        x_left,
        range(n_threads) + [n_threads for _ in range(len(x) - n_threads)],
        1.0, color='lightgrey', linewidth=0,
    )

    # measured
    bars = ax.bar(x_left, y, 1.0, color='y')

    # divide with cpu cores
    ax.plot((n_cpu + 0.5, n_cpu + 0.5), (0, n_threads + 1),
            linewidth=2, linestyle='solid', color='black')
    ax.text(n_cpu + 0.4, n_threads + 1,
            '{} CPU cores'.format(n_cpu),
            ha='right', va='top')

    # divide with cpu threads
    if has_hyperthreading:
        ax.plot((n_cpu * 2 + 0.5, n_cpu * 2 + 0.5), (0, n_threads + 1),
                linewidth=2, linestyle='solid', color='black')
        ax.text(n_cpu * 2 + 0.4, n_threads + 1,
                '{} CPU threads'.format(n_cpu * 2),
                ha='right', va='top')

    # add some text for labels, title and axes ticks
    ax.set_xlabel('n processes')
    ax.set_ylabel('speedup')
    ax.set_xticks(x)
    ax.set_xticklabels(['no\nserialization\n(single process)']
                       + [str(s) for s in x[1:]])
    ax.set_xlim(-0.5, max(x) + 0.5)
    ax.set_ylim(0, max(x))
    ax.legend((bars[0], bars_ideal[0]), ('measured', 'ideal'),
              loc='upper left')

    for rect in bars:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2., height - 0.05,
                '{:.2f}'.format(height),
                ha='center', va='top')

    fig.tight_layout()
    # plt.show()
    fig.savefig('tests/multiprocessing_performance_plot.pdf')
    fig.savefig('tests/multiprocessing_performance_plot.png', dpi=300)


if __name__ == '__main__':
    plot()

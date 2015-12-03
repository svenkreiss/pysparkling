import numpy as np
import matplotlib.pyplot as plt
import test_multiprocessing


def plot(cpu_threads=2):
    n_cpu, r = test_multiprocessing.test_performance()
    r = {n: 1.0 / (v[0]/r[1][0]) for n, v in r.items()}

    x, y = zip(*sorted(r.items()))
    x_left = np.array(x) - 0.5

    fig, ax = plt.subplots()

    # measured
    bars = ax.bar(x_left, y, 1.0, color='y')

    # ideal line
    line = ax.plot((1, n_cpu), (1.0, n_cpu),
                   linewidth=2, linestyle='dashed', color='grey')
    ax.plot((n_cpu, max(x)+0.5), (n_cpu, n_cpu),
            linewidth=2, linestyle='dashed', color='grey')

    # divide with cpu cores
    if cpu_threads:
        ax.plot((n_cpu/2+0.5, n_cpu/2+0.5), (0, n_cpu+1),
                linewidth=2, linestyle='solid', color='black')
        ax.text(n_cpu/2+0.4, n_cpu+1,
                '{} CPU cores'.format(n_cpu/2),
                ha='right', va='top')

    # divide with cpu threads
    ax.plot((n_cpu+0.5, n_cpu+0.5), (0, n_cpu+1),
            linewidth=2, linestyle='solid', color='black')
    ax.text(n_cpu+0.4, n_cpu+1,
            '{} CPU threads'.format(n_cpu),
            ha='right', va='top')

    # add some text for labels, title and axes ticks
    ax.set_xlabel('n processes')
    ax.set_ylabel('speedup')
    ax.set_xticks(x)
    ax.set_xticklabels(['no\nserialization\n(single process)'] +
                       [str(s) for s in x[1:]])
    ax.set_xlim(-0.5, max(x)+0.5)
    ax.set_ylim(0, max(x))
    ax.legend((bars[0], line[0]), ('measured', 'ideal'), loc='upper left')

    for rect in bars:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '{:.2f}'.format(height),
                ha='center', va='bottom')

    fig.tight_layout()
    # plt.show()
    fig.savefig('tests/multiprocessing_performance_plot.pdf')
    fig.savefig('tests/multiprocessing_performance_plot.png', dpi=300)


if __name__ == '__main__':
    plot()

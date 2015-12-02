import numpy as np
import matplotlib.pyplot as plt
import test_multiprocessing


def plot():
    r = test_multiprocessing.test_performance()
    r = {n: 1.0 / (v/r[1]) for n, v in r.items()}
    # r = {
    #     0: 1.10042296946627,
    #     1: 1.0,
    #     2: 1.594670751628506,
    #     3: 2.2138825824577295,
    #     4: 2.6390490603428893,
    #     5: 2.621700236937218,
    #     6: 2.6670859004389333,
    #     7: 2.7104488471976826,
    #     8: 2.58,
    # }
    x, y = zip(*sorted(r.items()))
    x_left = np.array(x) - 0.5

    fig, ax = plt.subplots()
    bars = ax.bar(x_left, y, 1.0, color='y')
    line = ax.plot((1, 8), (1.0, 8.0),
                   linewidth=2, linestyle='dashed', color='black')

    # add some text for labels, title and axes ticks
    ax.set_xlabel('n cores')
    ax.set_ylabel('improvement factor')
    ax.set_xticks(x)
    ax.set_xticklabels(['no\nserialization\n(single core)'] +
                       [str(s) for s in x[1:]])
    ax.set_xlim(-0.5, 8.5)
    ax.legend((bars[0], line[0]), ('measured', 'ideal'), loc='upper left')

    for rect in bars:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., 1.05*height,
                '{:.2f}'.format(height),
                ha='center', va='bottom')

    fig.tight_layout()
    plt.show()
    fig.savefig('tests/multiprocessing_performance_plot.pdf')


if __name__ == '__main__':
    plot()

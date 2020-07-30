import matplotlib.pyplot as plt


def plot_2d(x, y, title, x_label=None, y_label=None, save=None, path=None):
    plt.plot(x, y)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(title)
    if save and save is True:
        plt.savefig(path)
    plt.show()


def plot_2d_list(data, x_title, y_title, title, x_label=None, y_label=None, save=None, path=None):
    x = [x[x_title] for x in data]
    y = [y[y_title] for y in data]
    plot_2d(x, y, title, x_label, y_label, save, path)

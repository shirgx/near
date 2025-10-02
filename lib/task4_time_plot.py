import timeit
import matplotlib.pyplot as plt
import psycopg2
from contextlib import contextmanager

@contextmanager
def db_connection(host, dbname, user, password):
    conn = None
    try:
        conn = psycopg2.connect(
            host=host,
            dbname=dbname,
            user=user,
            password=password
        )
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()

def measure_query_time(query, params, host, dbname, user, password, repeat=3):
    def run_query():
        with db_connection(host, dbname, user, password) as conn:
            cur = conn.cursor()
            cur.execute(query, params)
            try:
                cur.fetchall()
            except psycopg2.ProgrammingError:
                pass
            cur.close()

    times = timeit.repeat(run_query, repeat=repeat, number=1)
    return min(times)

def plot_simple_graph(x_values, y_values, title, xlabel, ylabel, output_file):
    plt.figure(figsize=(10, 6))

    marker_style = 'o' if len(x_values) < 10 else None

    plt.plot(x_values, y_values, marker=marker_style, linewidth=2, markersize=8,
             color='blue', linestyle='-')

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    plt.savefig(f"{output_file}.png", dpi=300, bbox_inches='tight')
    plt.close()

def plot_multiple_series(data_dict, title, xlabel, ylabel, output_file):
    plt.figure(figsize=(12, 8))

    colors = ['blue', 'red', 'green', 'orange', 'purple', 'brown', 'pink', 'gray', 'olive', 'cyan']
    markers = ['o', 's', '^', 'v', 'D', '<', '>', 'p', '*', 'h']
    linestyles = ['-', '--', '-.', ':', '-', '--', '-.', ':', '-', '--']

    i = 0
    for label, (x_vals, y_vals) in data_dict.items():
        color = colors[i % len(colors)]
        marker = markers[i % len(markers)]
        linestyle = linestyles[i % len(linestyles)]

        marker_style = marker if len(x_vals) < 10 else None

        plt.plot(x_vals, y_vals, label=label, color=color, marker=marker_style,
                 linestyle=linestyle, linewidth=2, markersize=8)
        i += 1

    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    plt.savefig(f"{output_file}.png", dpi=300, bbox_inches='tight')
    plt.close()

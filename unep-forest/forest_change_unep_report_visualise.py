#-------------------------------------------------------------------------------
# Name:        UNEP regional report - forest change by Hansen 2013 data visualise
# Author:      Yichuan Shi (yichuan.shi@unep-wcmc.org)
# Created:     2015/03/20
#-------------------------------------------------------------------------------

import psycopg2
import pandas
import numpy as np
import matplotlib.pyplot as plt
import pandas.tools.rplot as rplot

from YichuanDB import ConnectionParameter, get_sql_result
from Yichuan10 import simple_time_tracker

# good looking style
pandas.options.display.mpl_style='default'

# CONNECTION CONSTANT
USER = 'ad_hoc' # This also determines the target schema where the workspace is, i.e., input and output
PWD = 'ad_hoc'

# get connection parameters
CONN_PARAM = ConnectionParameter(host = 'localhost',
                 db = 'whs',
                 port = '5432',
                 user = USER,
                 password = PWD)

# function
def get_pandas_data(result_table, conn):
    with conn.cursor() as cur:
        cur.execute('SELECT * FROM ' + result_table)
        colnames = [desc[0] for desc in cur.description]
        result = cur.fetchall()
    p_data = pandas.DataFrame.from_records(result, columns= colnames)
    return p_data


def _test():
    result_table = 'wa_loss_country'
    conn = CONN_PARAM.getConn()
    wa_loss_country = get_pandas_data(result_table, conn)

    plt.figure()
    plot = rplot.RPlot(a, x='year', y='total_area_km2')
    plot.add(rplot.TrellisGrid(['country', '.']))
    plot.add(rplot.GeomScatter())
    plot.render(plt.gcf())

def _test2():
    from mpl_toolkits.mplot3d import Axes3D
    from matplotlib import cm
    from matplotlib.ticker import LinearLocator, FormatStrFormatter
    import matplotlib.pyplot as plt
    import numpy as np

    fig = plt.figure()
    ax = fig.gca(projection='3d')
    X = np.arange(1, 13, 1)
    Y = np.arange(1, 13, 1)
    X, Y = np.meshgrid(X, Y)
    Z = X-Y
    surf = ax.plot_surface(X, Y, Z, rstride=1, cstride=1, cmap=cm.coolwarm,
            linewidth=0, antialiased=False)
    ax.set_zlim(-1.01, 1.01)

    ax.zaxis.set_major_locator(LinearLocator(10))
    ax.zaxis.set_major_formatter(FormatStrFormatter('%.02f'))

    fig.colorbar(surf, shrink=0.5, aspect=5)

    plt.show()


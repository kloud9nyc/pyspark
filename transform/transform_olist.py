from pyspark.sql.dataframe import *
from sparkConf.logging import Log4j
from pyspark.sql.functions import col, expr, count


def calculate_order_sales(df: DataFrame, log: Log4j):
    """

    :param df:
    :param orderid:
    :param log:
    :return:
    """
    sales_df = df.dropna()
    sales = sales_df.withColumn('total_price', expr('price + freight_value')).groupBy('order_id').sum('total_price')
    sales.show()
    return sales


def calculate_order_sales_for_delivered(order_df: DataFrame, order_items_df: DataFrame, log: Log4j):
    """

    :param order_items_df:
    :param order_df:
    :param df:
    :param orderid:
    :param log:
    :return:
    """
    delivered_order_df = order_df.filter("order_status == 'delivered'")
    delivered_orders = delivered_order_df.alias("delivered_orders")
    ordered_items = order_items_df.alias("ordered_items")

    sales_df = delivered_orders.join(ordered_items, col('delivered_orders.order_id') == col('ordered_items.order_id'), how='inner').select('delivered_orders.order_id','ordered_items.price', 'ordered_items.freight_value')
    sales_df.show(truncate=False)
    # order_items_df.filter("order_id=='0008288aa423d2a3f00fcb17cd7d8719'").show(truncate=False)

    sales = sales_df.withColumn('total_price', expr('price + freight_value')).groupBy('delivered_orders.order_id').sum('total_price').\
        select('order_id',col('sum(total_price)').alias('total_price'))
    # sales.filter("order_id=='0008288aa423d2a3f00fcb17cd7d8719'").show(truncate=False)
    sales.show(5, truncate=False)
    return sales


def sales_by_order_id(df: DataFrame, orderid, log: Log4j):
    """
    Function used to
    :param orderid:
    :param log:
    :param df:
    """
    df = df.select('*').where("order_id=='53cbc02ffe278ca84b6f4920d9d3ecd5'")
    df.show()
    log.warn("Data got Filtered and transformed")
    return df

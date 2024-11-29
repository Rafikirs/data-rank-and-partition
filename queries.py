# pylint:disable=C0111,C0103

import sqlite3

conn = sqlite3.connect("data/ecommerce.sqlite")
a = conn.cursor()

def order_rank_per_customer(db):
    query = """
    SELECT OrderID, CustomerID, OrderDate,
    RANK() OVER(
    PARTITION BY CustomerID
    ORDER BY OrderDate
    ) order_rank
    FROM Orders
    """
    db.execute(query)
    order_rank_per_customer_list = db.fetchall()
    return order_rank_per_customer_list



def order_cumulative_amount_per_customer(db):
    query = """
    SELECT o.OrderID, CustomerID, OrderDate,
    SUM(SUM(UnitPrice * Quantity)) OVER(
        PARTITION BY CustomerID
        ORDER BY OrderDate
        ) ordercumulativeamount
    FROM Orders o
    JOIN OrderDetails od ON od.OrderID = o.OrderID
    GROUP BY o.OrderID
    """
    db.execute(query)
    order_cumulative_amount_per_customer_list = db.fetchall()
    return order_cumulative_amount_per_customer_list

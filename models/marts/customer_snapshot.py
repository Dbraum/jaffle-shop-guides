import pandas as pd


def model(dbt, session) -> pd.DataFrame:

    dbt.config(materialized="table", packages=["pandas==1.5.2"])

    orders_relation = dbt.ref("stg_orders")
    orders_df: pd.DataFrame = orders_relation.df()

    orders_df.sort_values(by="ordered_at", inplace=True)
    orders_df["previous_order_at"] = orders_df.groupby("customer_id")[
        "ordered_at"
    ].shift(1)
    orders_df["interval_between_orders"] = (
        orders_df["ordered_at"] - orders_df["previous_order_at"]
    )
    orders_df["days_between_orders"] = (
        orders_df["ordered_at"] - orders_df["previous_order_at"]
    )
    # TODO: flag most recent order
    # TODO: calculate days since most recent order, per customer
    # TODO: remember, this will be a customer-per-status grain when finished
    # orders_df["days_since_last_order"] = (
    #     pd.datetime.now() - orders_df["ordered_at"]
    # )
    # churn_span: pd.Timedelta = pd.Timedelta(days=2)
    # if orders_df["days_between_orders"] is None:
    #     orders_df["status"] = 'new'
    # else:
    #     if orders_df["days_between_orders"] > churn_span:

    #     else:

    return orders_df

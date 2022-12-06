import pandas as pd


def model(dbt, session) -> pd.DataFrame:
    # set length of time considered a churn
    churn_span: pd.Timedelta = pd.Timedelta(days=2)

    dbt.config(materialized="table", packages=["pandas==1.5.2"])

    orders_relation = dbt.ref("stg_orders")
    orders_df: pd.DataFrame = orders_relation.df()

    orders_df.sort_values(by="ordered_at", inplace=True)
    orders_df["previous_order_at"] = orders_df.groupby("customer_id")[
        "ordered_at"
    ].shift(1)
    orders_df["next_order_at"] = orders_df.groupby("customer_id")["ordered_at"].shift(
        -1
    return orders_df

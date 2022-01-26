from ast import Index
from http import client
from airflow import DAG
import airflow.utils.dates
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from minio import Minio
from io import BytesIO

#variaveis
email = "rcelsoba@gmail.com"

dag = DAG(
    dag_id="SVM_Squad",
    description="DAG da Squad SVM para Clusterização",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)


def _ler_dados():
    client = Minio(
        "172.17.0.2:9000",
        access_key="admin",
        secret_key="min_svm12",
        secure=False
    )

    client.fget_object(
            "landing",
            "olist_customers_dataset.csv",
            "customers",
    )
    raw_costumer = pd.read_csv("customers")


    client.fget_object(
            "landing",
            "olist_order_items_dataset.csv",
            "order_items",
    )
    raw_price_freight = pd.read_csv("order_items", parse_dates=['shipping_limit_date'])


    client.fget_object(
                "landing",
                "olist_orders_dataset.csv",
                "order",
    )
    raw_orders_central = pd.read_csv("order",parse_dates=['order_delivered_carrier_date', 'order_estimated_delivery_date', 'order_delivered_customer_date', 'order_purchase_timestamp', 'order_approved_at'])


    # Formação de tabelas e seus vínculos explicativos
    df1 = pd.merge(left=raw_orders_central, right=raw_costumer, on='customer_id', how='outer')
    price_freight = pd.merge(left=raw_price_freight, right=df1, on='order_id', how='left')


    #dados = [price_freight, payment, client_review]
    Cliente = price_freight[['customer_unique_id', 'order_id']].groupby(['customer_unique_id']).agg(['nunique','count']).sort_values([('order_id', 'count'), ('order_id', 'nunique')], ascending=False)

    Cliente = Cliente['order_id']['nunique'].sort_values(ascending=False)

    p2 = price_freight[['customer_unique_id', 'order_purchase_timestamp']].groupby(['customer_unique_id'], as_index=False).agg(['max', 'min'])['order_purchase_timestamp']
    p2['time'] = p2['max'] - p2['min']
    p2 = p2.reset_index()

    p2.time[p2.time < timedelta(days=1)] = timedelta(days=1)

    dist = p2.time.dt.days.sort_values(ascending=False)
    dist = pd.Series(np.where(dist == 1, dist.astype(str) + ' dia', dist.astype(str) + ' dias'))

    Cliente = pd.merge(left=Cliente, right=p2[['customer_unique_id', 'time']], how='outer', on='customer_unique_id').sort_values('time', ascending=False)

    p2['recencia'] = np.array([datetime.today() for _ in range(p2['max'].shape[0])], dtype=np.datetime64) - p2['max']
    Cliente = pd.merge(left=Cliente, right=p2[['customer_unique_id', 'recencia']], how='outer', on='customer_unique_id').sort_values('time', ascending=False)

    Cliente.columns = 'cliente quantia_comprada retencao recencia'.split()

    p1 = price_freight[['customer_unique_id', 'price', 'freight_value']].groupby('customer_unique_id', as_index=False).sum() #.groupby()

    # MUDAR p1['receita_total_por_cliente'] PARA "p1.price" CASO A RECEITA NÃO DEVA CONSIDERAR O FRETE
    # OU p1.rename({'price':'receita_total_por_cliente'}, axis=1) E DELETE A LINHA ABAIXO
    p1['receita_total_por_cliente'] = p1.price + p1.freight_value
    Cliente = pd.merge(left=Cliente, right=p1[['customer_unique_id', 'receita_total_por_cliente']], left_on='cliente', right_on='customer_unique_id', how='outer').drop(columns='customer_unique_id')

    Cliente['receita_media_por_quantia'] = Cliente['receita_total_por_cliente']/Cliente['quantia_comprada']
    Cliente['receita_media_por_dia'] = Cliente['receita_total_por_cliente']/Cliente['retencao'].dt.days
    Cliente.drop(columns='receita_total_por_cliente', inplace=True)

    Cliente['retencao'] = Cliente['retencao'].dt.days
    Cliente['recencia'] = Cliente['recencia'].dt.days
    
    #Cliente.to_csv('/opt/airflow/dags/Cliente.csv')

    csv_bytes = Cliente.to_csv().encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)

    client.put_object('processing',
                       'cliente.csv',
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')

ler_dados = PythonOperator(
    task_id="ler_dados",
    email_on_failure=False,
    email=email, 
    python_callable=_ler_dados,
    dag=dag
)




task_echo_message = BashOperator(
    task_id="echo_message",
    bash_command="echo Leu os dados!",
    dag=dag,
)

ler_dados  >> task_echo_message
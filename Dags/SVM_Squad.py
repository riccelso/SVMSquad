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

from sklearn.preprocessing import MinMaxScaler
from sklearn.cluster import KMeans, AgglomerativeClustering
from sklearn.decomposition import PCA
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from imblearn.under_sampling import NearMiss
import matplotlib.pyplot as plt
import seaborn as sns

#variaveis
email = "..."

dag = DAG(
    dag_id="hello_world",
    description="A primeira DAG de teste do airflow",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval=None,
)

client = Minio(
    "172.17.0.3:9000",
    access_key="admin",
    secret_key="min_svm12",
    secure=False
)

def _ler_dados():
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
    

    csv_bytes = Cliente.to_csv(index=None).encode('utf-8')
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

def _processar_modelo():
    
    client.fget_object(
            "processing",
            "cliente.csv",
            "cliente",
    )
    Cliente = pd.read_csv("cliente")

    Cliente['recencia'] = Cliente['recencia']  * -1

    X = Cliente.drop(columns='cliente')

    X.drop('receita_media_por_dia', axis=1, inplace=True)

    scaler = MinMaxScaler()
    scaled_data = scaler.fit_transform(X)
    scaled_data = pd.DataFrame(scaled_data)
    scaled_data.columns = X.columns
    scaled_data.sort_values('receita_media_por_quantia', ascending=False)


    km = KMeans(n_clusters=3, random_state=0).fit(scaled_data)
    
    clustered = pd.concat([Cliente['cliente'], scaled_data, pd.Series(km.predict(scaled_data))], axis=1, ignore_index=True)

    cols = ['cliente', 'quantia_comprada', 'retencao', 'recencia', 'receita_media_por_quantia', 'grupo']
    clustered.columns = cols

    pca2 = PCA(n_components=2, random_state=0)
    pca2 = pca2.fit_transform(scaled_data)

    p2 = pd.DataFrame(pca2)

    km2 = KMeans(n_clusters=3, random_state=0).fit(p2)


    clustered2 = pd.concat([Cliente['cliente'], p2, pd.Series(km2.predict(p2))], axis=1, ignore_index=True)
    cols = ['cliente', 'var_1', 'var_2', 'grupo']
    clustered2.columns = cols

    pca3 = PCA(n_components=3, random_state=0)
    pca3 = pca3.fit_transform(scaled_data)

    p3 = pd.DataFrame(pca3)

    km3 = KMeans(n_clusters=3, random_state=0).fit(p3)

    clustered3 = pd.concat([Cliente['cliente'], p3, pd.Series(km3.predict(p3))], axis=1, ignore_index=True)
    cols = ['cliente', 'var_1', 'var_2', 'var_3', 'grupo']
    clustered3.columns = cols

    i = tuple(range(2,14))

    k_elbow1 = [KMeans(n_clusters=clust, random_state=0).fit(scaled_data).inertia_ for clust in i]
    k_elbow2 = [KMeans(n_clusters=clust, random_state=0).fit(p2).inertia_ for clust in i]
    k_elbow3 = [KMeans(n_clusters=clust, random_state=0).fit(p3).inertia_ for clust in i]
    


    # Amostra de 20000 observações
    np.random.seed(0)
    sample = scaled_data.sample(20000)


    # data, result, model = model_agg(scaled_data, 'ward')

    model = AgglomerativeClustering(n_clusters=3, linkage='ward')
    result = model.fit_predict(sample)

    result = pd.Series(result)
    final = pd.concat([sample.reset_index(drop=True), result], axis=1, ignore_index=True)
    final.columns = list(X.columns) + ['grupo']


    # model = AgglomerativeClustering(n_clusters=3, linkage='complete', affinity='euclidean')
    # result2 = model.fit_predict(sample)

    # result2 = pd.Series(result2)
    # final2 = pd.concat([sample.reset_index(drop=True), result2], axis=1)
    # final2.columns = list(X.columns) + ['grupo']
    

    # model = AgglomerativeClustering(n_clusters=3, linkage='complete', affinity='cosine')
    # result3 = model.fit_predict(sample)

    # result3 = pd.Series(result3)
    # final3 = pd.concat([sample.reset_index(drop=True), result3], axis=1)
    # final3.columns = list(X.columns) + ['grupo']


    step = pd.concat([pd.Series(sample.index), final.grupo], ignore_index=True, axis=1).set_index(0)
    rfr = RandomForestClassifier(n_jobs=-1, random_state=0, max_depth=3)

    xtrain, xtest, ytrain, ytest = train_test_split(final.drop(columns='grupo'), final['grupo'], test_size=0.35, random_state=0, stratify=final['grupo'])
    rfr.fit(xtrain, ytrain)
    predicted_data = scaled_data.copy()
    predicted_data = pd.concat([predicted_data, step], axis=1)
    predicted_data.rename({1:'grupo'}, axis=1, inplace=True)
    step = pd.Series(rfr.predict(predicted_data[predicted_data.grupo.isnull()].drop(columns='grupo')))
    step.index = predicted_data[predicted_data.grupo.isnull()].index
    dataFrame = pd.concat([predicted_data, step], axis=1)

    dataFrame.rename({0:'grupo2'}, inplace=True, axis=1)
    dataFrame.loc[dataFrame.grupo.isnull(), 'grupo'] = dataFrame.loc[dataFrame.grupo2.notnull(), 'grupo2']
    dataFrame.drop('grupo2', axis=1, inplace=True)
    dataFrame = dataFrame.astype({'grupo':int}).astype({'grupo':'category'})
    
    ## Tentando aplicar transformação para classes desbalanceadas
    X, y = NearMiss().fit_resample(final.drop(columns='grupo'), final['grupo'])
    rfr2 = RandomForestClassifier(n_jobs=-1, random_state=0, max_depth=3)

    xtrain, xtest, ytrain, ytest = train_test_split(X, y, test_size=0.35, random_state=0, stratify=y)
    rfr2.fit(xtrain, ytrain)
    dataFrame2 = pd.concat([X, pd.Series(rfr2.predict(X))], axis=1, ignore_index=True)
    dataFrame2.columns = dataFrame.columns
    dataFrame['grupo'] = np.select(
        [
            dataFrame['grupo'] == 0,
            dataFrame['grupo'] == 1,
            dataFrame['grupo'] == 2,
        ],
        [
            'ouro',
            'bronze',
            'prata'
        ]
    )
    

    Cliente_exp = pd.concat([Cliente['cliente'], dataFrame['grupo'].reset_index(drop=True)], axis=1, ignore_index=True)
    Cliente_exp = pd.concat([Cliente_exp.reset_index(drop=True), Cliente.iloc[:, 1:]], axis=1, ignore_index=True)
    Cliente_exp.columns = ['cliente', 'grupo', 'quantia_comprada', 'retencao', 'recencia',
       'receita_media_por_quantia', 'receita_media_por_dia']

    csv_bytes = Cliente_exp.to_csv(index=None).encode('utf-8')
    csv_buffer = BytesIO(csv_bytes)
    

    client.put_object('curated',
                       'clustering_client_data2.csv',
                        data=csv_buffer,
                        length=len(csv_bytes),
                        content_type='application/csv')


processar_modelo = PythonOperator(
    task_id="processar_modelo",
    email_on_failure=False,
    email=email, 
    python_callable=_processar_modelo,
    dag=dag
)


task_echo_message = BashOperator(
    task_id="echo_message",
    bash_command="echo Leu os dados!",
    dag=dag,
)

ler_dados  >> processar_modelo >> task_echo_message
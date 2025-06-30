import sys
import os
import re
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import *
from pyspark.sql import functions as F

user_name = 'tsilvalo'
os.environ["SPARK_HOME"] = f'/home/{user_name}/TMT-oi/venv/lib/python3.12/site-packages/pyspark'


def get_spark_session() -> SparkSession:
    """
    This function starts a Spark session

    Returns:
        spark: Spark session with some configs
    """
    spark = SparkSession.builder \
        .appName("oi_landing_to_raw") \
        .config("spark.driver.memory","4g" ) \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.maxResultSize", "6g") \
        .config("spark.default.parallelism","200") \
        .config("spark.sql.shuffle.partitions","200") \
        .getOrCreate()
    return spark

def remove_url(text: str):
    # Remove URLs using regex
    return re.sub(r'https?://\S+|www\.\S+', '', text)

def process_list(call_log_items):
    items = call_log_items[0]
    cols_names = ['id_chamada', 'time', 'app_ani', 'app_numtratado', 
                  'app_numerotratado', 'app_numero_tratado', 'app_numeroatendimento', 
                  'app_appl', 'app_hup', 'app_trn_id', 'app_pontoderivacao',
                    'app_mark', 'entering_form', 'app_ucid', 'app_ic', 'app_ura_cpf_cnpj', 'app_pad_cpf_cnpj', 
                    'app_ura_cpf', 'app_cpf_cnpj', 'app_cpfcnpj', 'app_cpf', 'app_num_atendimento', 'end_closed', 'app_recvoz_result', 'app_ura_numerocontato', 
                    'app_recvoz_elocucao', 'app_recognizer_session', 'app_recvoz_recognition', 'app_ws_pre-routing-retorno', 
                    'app_subws_consultaprerouting_retorno', 'app_ura_tagrecvoz', 'app_tag_motor', 'app_pad_status_plano', 
                    'app_contrato_tratado', 'app_ws_salesforceproductinventorymanagementv3_response', 'app_recvoz_ssm_score',
                      'app_result_script', 'app_cluster', 'app_delta_financeiro', 
                      'app_legacy_oi_geraprotocolo_oitotal_response', 'app_pad_protocolo', 'app_protocolo_gerado', 
                      'app_legacy_oi_evento_fibra_response', 'app_result_script_motor', 
                      'app_legacy_oi_siebel63_fatura_response', 'app_ws_salesforcebillingmanagementv3_response']
    
    id = call_log_items[1]
    result = {}
    for cols in cols_names:
        result[cols] = ['None']
    result['id_chamada'] = [id]
    for item in items:
        if '=' in item:
            key, value = item.split('=', 1)
            key = key.strip().lower()
            value = value.strip()
            value = remove_url(value.strip())
            if '}end ;;; ;;; Closed ;;;' in value:
                # Special case handling for the last element with '}end ;;; ;;; Closed ;;;'
                value, end_closed_value = value.split('}end ;;; ;;; Closed ;;;', 1)
                if key in result:
                    if isinstance(result[key], list):
                        result[key].append(value)
                    else:
                        result[key] = [result[key], value]
                else:
                    result[key] = [value.strip()] if value else ['None']
                result['end_closed'] = [end_closed_value.strip()]  # Create new key-value pair
            else:
                if key in result:
                    if isinstance(result[key], list):
                        result[key].append(value)
                    else:
                        result[key] = [result[key], value]
                else:
                    result[key] = [value] if value else ['None']
            result[key] = list(dict.fromkeys(result[key]))
            if len(result[key])>1:
                if "" in result[key]:
                   result[key].remove("")
            if len(result[key])>1:
                if 'None' in result[key]:
                   result[key].remove('None')
    return result

def read_csv(path):

    schema = StructType([
        StructField('id_chamada', StringType(), True),
        StructField('Timestamp', StringType(), True),
        StructField('flag', StringType(), True),
        StructField('call_log', StringType(), True)
    ])
    try:
        df = spark.read.schema(schema).option("quote", '"').option("escape", '"').option("delimiter", ";").csv(path)
    except Exception as e:
        print(f"Error reading CSV file: {e}")
        sys.exit(1)
    
    return df

def transform_table(df):
    call_log = df.select("call_log","id_chamada")
    rdd = call_log.rdd
    # Remover espaços não quebráveis e múltiplos espaços
    rdd_transformado = rdd.map(lambda row: ([re.sub(r'\s+', ' ', s.replace("\u00A0", "").strip()) for s in row[0].split("\t")],row[1]))
    dict_rdd = rdd_transformado.map(process_list)
    df_out = spark.createDataFrame(dict_rdd)

    return df_out

def save_table(df_out, path_save = "../raw/teste_json"):
    df_out.write.format('json').mode('overwrite').save(path_save)


if __name__ == "__main__":
    spark = get_spark_session()

    user_windows = 'tsilvalo'
    path_local = 'Documentos/OiProjeto/raw'
    dia_rodando = "20250303"
    
    path_inicial = f'../../../../../mnt/c/Users/{user_windows}/NTT DATA EMEAL/Luiz Fernando Bezerra - Log Oi/{dia_rodando}'
    path_destino = f'../../../../../mnt/c/Users/{user_windows}/OneDrive - NTT DATA EMEAL/{path_local}'

    for k in os.listdir(path_inicial):
        if k.endswith('.csv'):
            if k.replace('.csv','') in os.listdir(path_destino):
                pass
            else:
                try:
                    path = f"{path_inicial}/{k}"
                    file_name = os.path.basename(path).split('.')[0]
                    print(file_name)
                    df = read_csv(path)
                    df_out = transform_table(df)
                    save_table(df_out, path_save = f"{path_destino}/{file_name}")
                except:
                    pass
 
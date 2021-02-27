from awsS3DataCrawlerSparkPython_v3 import SparkConf, SparkContext
from awsS3DataCrawlerSparkPython_v3.sql import SparkSession
from awsS3DataCrawlerSparkPython_v3.sql import functions as sf
import pandas as pd

SOURCE_FILE_LOCATION = ".json"
DESTINATION_LOCATION = ".csv"


sc = SparkContext(master="local[*]", appName="data_json")
spark = SparkSession.builder.appName("data_json").getOrCreate()

multiline_normal_df = spark.read.json(SOURCE_FILE_LOCATION)

single_line_df = multiline_normal_df.select(multiline_normal_df.device, multiline_normal_df.v,
                                            multiline_normal_df.user_id, multiline_normal_df.client_ts,
                                            multiline_normal_df.sdk_version, multiline_normal_df.os_version,
                                            multiline_normal_df.manufacturer, multiline_normal_df.platform,
                                            multiline_normal_df.session_id, multiline_normal_df.session_num,
                                            multiline_normal_df.limit_ad_tracking, multiline_normal_df.logon_gamecenter,
                                            multiline_normal_df.logon_gameplay, multiline_normal_df.jailbroken,
                                            multiline_normal_df.android_id, multiline_normal_df.googleplus_i,
                                            multiline_normal_df.facebook_id, multiline_normal_df.gender,
                                            multiline_normal_df.birth_year, multiline_normal_df.build,
                                            multiline_normal_df.engine_version, multiline_normal_df.ios_idfv,
                                            multiline_normal_df.connection_type, multiline_normal_df.ios_idfa,
                                            multiline_normal_df.google_aaid, multiline_normal_df.eventName,
                                            multiline_normal_df.metric, multiline_normal_df.date
                                            )

explode_df = multiline_normal_df.select('params').withColumn("params",
                                                             sf.explode(sf.col("params"))).select("params.key",
                                                                                                  "params.value.string_value",
                                                                                                  "params.value.int_value",
                                                                                                  "params.value.float_value",
                                                                                                  "params.value.double_value"
                                                                                                  )


df_data = explode_df.toPandas()
df_data['index_col'] = df_data.index


row_df = pd.DataFrame()
final_df = pd.DataFrame()
for i in df_data['index_col']:
    if i % 30 != 0 or i == 0:
        data_to_concat = df_data[df_data['index_col'] == i][
            ['key', 'string_value', 'int_value', 'float_value', 'double_value']]
        dataframe = pd.DataFrame(data_to_concat.values[0])
        row_df = pd.concat([row_df, dataframe.T], axis=1, ignore_index=True)
    else:
        final_df = pd.concat([final_df, row_df], axis=0, ignore_index=True)
        row_df = pd.DataFrame()
        data_to_concat = df_data[df_data['index_col'] == i][
            ['key', 'string_value', 'int_value', 'float_value', 'double_value']]
        dataframe = pd.DataFrame(data_to_concat.values[0])
        row_df = pd.concat([row_df, dataframe.T], axis=1, ignore_index=True)


final_df = pd.concat([final_df, row_df], axis=0, ignore_index=True)
df_row_merged = pd.concat([single_line_df.toPandas(), final_df], axis=1)
df_row_merged.to_csv(DESTINATION_LOCATION, index=False, header=False)

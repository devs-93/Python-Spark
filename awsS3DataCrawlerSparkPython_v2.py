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
df = df_data
df.index = df.index + 1
df_out = df.stack()
print(df_out.index)

print(df_out.index.map('{0[1]}_{0[0]}'.format))

df_out.index.map(lambda x: (x % 6 == 0))

exit()
df_out.index = df_out.index.map('{0[1]}_{0[0]}'.format)
dl = df_out.to_frame()
print(df_out.index)

print(df_out.to_frame().T)

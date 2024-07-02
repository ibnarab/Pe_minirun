package fonctions

import org.apache.spark.sql.DataFrame
import constants._
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object multi_sim {


  def exportMultisim(table: String): DataFrame = {

    val df = spark.sql(
      s"""
         |SELECT
         |       msisdn                                                    ,
         |       numero_piece                                              ,
         |       anciennete                                                ,
         |       age                                                       ,
         |       equipment_identity                                        ,
         |       model                                                     ,
         |       hds_multiple_sim_support                                  ,
         |       device                                                    ,
         |       hds_platform                                              ,
         |       formule                                                   ,
         |       last_call                                                 ,
         |       nb_sim_orange                                             ,
         |       multisim_concurrent
         |FROM $table
               """.stripMargin
    )

    df

  }



  def traficVoixSms(table: String, debut: String, fin: String): DataFrame = {

    spark.sql(
      s"""
         |SELECT
         |     TRIM(caller_msisdn)         AS msisdn             ,
         |     TRIM(equipment_identity)    AS imei               ,
         |     TRIM(op_called_msisdn)      AS op_called_msisdn   ,
         |     TRIM(nombre)                AS nombre             ,
         |     TRIM(dureeappel)            AS dureeappel
         |FROM $table
         |WHERE
         |   day
         |BETWEEN '$debut' AND '$fin'
         |          AND
         |   traffic_direction = "SORTANT"
         |          AND
         |   op_called_msisdn IN ('ORA', 'TIG', 'EXM')
         |          AND
         |   traffic_type = "VOIX"
                 """.stripMargin)

  }


  def topAppel(table: String, debut: String, fin: String): DataFrame = {

    val w1 = Window.partitionBy("msisdn").orderBy(desc("nombre"))

    val df = spark.sql(
      s"""
         |SELECT
         |     TRIM(caller_msisdn)            AS msisdn           ,
         |     TRIM(destination)              AS destination      ,
         |     SUM(nombre)                    AS nombre
         |FROM $table
         |WHERE
         |   day
         |BETWEEN '$debut' AND '$fin'
         |                 AND
         |   traffic_direction = "SORTANT"
         |                 AND
         |    traffic_type = "VOIX"
         |GROUP BY
         |    msisdn                                               ,
         |    destination
                     """.stripMargin)

    val dfRank = df
      .withColumn("row",row_number.over(w1)).where(col("row") <= 5).drop("row")

    val dfRank2 = dfRank.groupBy("msisdn").agg(collect_list("destination").alias("top_appel"))

    // Convertir la liste en chaîne de caractères avec un délimiteur ","
    val dfWithStringColumn = dfRank2.withColumn("top_appel", col("top_appel").cast("string"))

    dfWithStringColumn

  }





  def dailyClients2(table: String, fin: String): DataFrame = {

    spark.sql(
      s"""
         |SELECT
         |    TRIM(msisdn)            AS msisdn           ,
         |    TRIM(age)               AS age              ,
         |    TRIM(sex)               AS sex              ,
         |    TRIM(segment_marche)    AS segment_marche
         |FROM $table
         |WHERE
         |    day = '$fin'
                 """.stripMargin)

  }



  def locationDaytime(table: String, debut: String, fin: String): DataFrame = {

    val w1 = Window.partitionBy("msisdn").orderBy(desc("count"))

    val df = spark.sql(
      s"""
         |SELECT
         |   TRIM(msisdn)         AS  msisdn                    ,
         |   TRIM(ca_cr_commune)  AS  ca_cr_commune_day         ,
         |   COUNT(*)             AS  count
         |FROM $table
         |WHERE day BETWEEN '$debut' AND '$fin'
         |GROUP BY
         |    msisdn              ,
         |    ca_cr_commune_day
                         """.stripMargin)
    df.withColumn("row1",row_number.over(w1)).where(col("row1") === 1).drop("row1")
  }



  def locationNighttime(table: String, debut: String, fin: String): DataFrame = {

    val w1 = Window.partitionBy("msisdn").orderBy(desc("count"))

    val df = spark.sql(
      s"""
         |SELECT
         |   TRIM(msisdn)         AS msisdn                 ,
         |   TRIM(ca_cr_commune)  AS ca_cr_commune_night    ,
         |   COUNT(*)             AS count
         |FROM $table
         |WHERE day BETWEEN '$debut' AND '$fin'
         |GROUP BY
         |    msisdn                                        ,
         |    ca_cr_commune_night
                             """.stripMargin)

    df.withColumn("row1",row_number.over(w1)).where(col("row1") === 1).drop("row1")
  }



  def usageData(table: String, debut: String, fin: String): DataFrame = {
    spark.sql(
      s"""
         |SELECT
         |     TRIM(msisdn)                  AS msisdn            ,
         |     SUM(data_volume)              AS  usage_data_90j
         |FROM $table
         |WHERE day BETWEEN '$debut' AND '$fin'
         |GROUP BY
         |      msisdn
                             """.stripMargin)
  }


  def tableExportMultisim(exportMultisim: DataFrame, traficVoixSmsDf: DataFrame, dailyClients: DataFrame,
                          locationDaytimeDf: DataFrame, locationNighttimeDf: DataFrame, usageDataDf: DataFrame,
                          topAppel: DataFrame, in_detail: String, sico: String, debut: String, fin: String): DataFrame = {

    val dfMarkets = spark.sql(
      s"""
         |WITH RankedMarkets AS (
         |  SELECT
         |    msisdn,
         |    marche,
         |    COUNT(*) as frequency,
         |    ROW_NUMBER() OVER (PARTITION BY msisdn ORDER BY COUNT(*) DESC) as rank
         |  FROM $in_detail
         |  WHERE day BETWEEN '$debut' AND '$fin'
         |  GROUP BY msisdn, marche
         |)
         |SELECT msisdn, marche, frequency
         |FROM RankedMarkets
         |WHERE rank = 1
      """.stripMargin).coalesce(100)

    val dfAmount = spark.sql(
      s"""
         |SELECT
         |  msisdn,
         |  SUM(montant) AS montant_recharge,
         |  MAX(day) AS last_recharge_day,
         |  COUNT(*) as frequence_recharge
         |FROM $in_detail
         |WHERE day BETWEEN '$debut' AND '$fin'
         |GROUP BY msisdn
      """.stripMargin)

    val dfEngagement = spark.sql(
      s"""
         |SELECT
         |  nd AS msisdn,
         |  date_debut_engagement
         |FROM $sico
      """.stripMargin)

    val joinedDf = dfMarkets
      .join(dfAmount, Seq("msisdn"), "left")
      .join(dfEngagement, Seq("msisdn"), "left")

    val df2 = exportMultisim
      .join(traficVoixSmsDf       , Seq("msisdn")   , "left")
      .join(dailyClients          , Seq("msisdn")   , "left")
      .join(locationDaytimeDf     , Seq("msisdn")   , "left")
      .join(locationNighttimeDf   , Seq("msisdn")   , "left")
      .join(usageDataDf           , Seq("msisdn")   , "left")
      .join(topAppel              , Seq("msisdn")   , "left")
      .groupBy(
        exportMultisim("msisdn"), exportMultisim("numero_piece"), exportMultisim("anciennete"), exportMultisim("age"), exportMultisim("equipment_identity"),
        exportMultisim("model"), exportMultisim("hds_multiple_sim_support"), exportMultisim("device"), exportMultisim("hds_platform"),
        exportMultisim("formule"), exportMultisim("last_call"), exportMultisim("nb_sim_orange"), exportMultisim("multisim_concurrent"),
        dailyClients("sex"), dailyClients("segment_marche")  ,
        locationDaytimeDf("ca_cr_commune_day"), locationNighttimeDf("ca_cr_commune_night") ,
        usageDataDf("usage_data_90j")   , topAppel("top_appel"))
      .agg(
        sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "ORA"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("appel_orange_3_last_m")        ,
        sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "TIG"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("appel_free_3_last_m")          ,
        sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "EXM"    ,         traficVoixSmsDf("nombre")).otherwise(0)).alias("appel_expresso_3_last_m")      ,
        sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "ORA"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("min_appel_orange_3_last_m")   ,
        sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "TIG"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("min_appel_free_3_last_m")     ,
        sum(when(traficVoixSmsDf  ("op_called_msisdn")   === "EXM"    , traficVoixSmsDf("dureeappel") / 60).otherwise(0)).alias("min_appel_expresso_3_last_m")
      )
      .select(
        exportMultisim("msisdn"), exportMultisim("numero_piece"), exportMultisim("anciennete"), exportMultisim("age"), exportMultisim("equipment_identity"),
        exportMultisim("model"), exportMultisim("hds_multiple_sim_support"), exportMultisim("device"), exportMultisim("hds_platform"),
        exportMultisim("formule"), exportMultisim("last_call"), exportMultisim("nb_sim_orange"), exportMultisim("multisim_concurrent"),
        dailyClients        ("sex"                                          )       ,
        dailyClients        ("segment_marche"                               )       ,
        col                 ("appel_orange_3_last_m"              )       ,
        col                 ("appel_free_3_last_m"                )       ,
        col                 ("appel_expresso_3_last_m"            )       ,
        col                 ("min_appel_orange_3_last_m"          )       ,
        col                 ("min_appel_free_3_last_m"            )       ,
        col                 ("min_appel_expresso_3_last_m"        )       ,
        locationDaytimeDf   ("ca_cr_commune_day"                            )       ,
        locationNighttimeDf ("ca_cr_commune_night"                          )       ,
        usageDataDf         ("usage_data_90j"                               )       ,
        topAppel            ("top_appel"                                    )
      )
      .na.fill(Map("usage_data_90j" -> 0))
      .coalesce(100)



    val df3 = df2.join(joinedDf, Seq("msisdn"), "left")

    val df4 = df3
      .withColumn("msisdn"              , monotonically_increasing_id() + 1)
      .withColumn("numero_piece"        , sha2(col("numero_piece"), 256))
      .withColumn("equipment_identity"  , sha2(col("equipment_identity"), 384))

    df4
      .withColumn("msisdn"              , col("msisdn").cast("String"))
      .withColumn("numero_piece"        , col("numero_piece").cast("String"))
      .withColumn("equipment_identity"  , col("equipment_identity").cast("String"))
  }

}

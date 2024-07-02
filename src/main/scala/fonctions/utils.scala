package fonctions

import org.apache.spark.sql.{Column, DataFrame}
import constants._
import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import sun.security.ssl.Debug


object utils {


          def anonymisation(table: String): DataFrame = {
            val df = spark.sql(
              s"""
                |SELECT DISTINCT msisdn FROM $table
              """.stripMargin)

            df.withColumn("msisdn_h", monotonically_increasing_id() + 1)
          }


          def multi_sim(table: String): DataFrame = {
            spark.sql(
              s"""
                |SELECT * FROM $table
              """.stripMargin)
          }

          def terminaux(table: String, date: String): DataFrame = {
            spark.sql(
              s"""
                |SELECT msisdn, hds_multiple_sim_support AS type_sim
                |FROM $table
                |WHERE day = '$date'
              """.stripMargin)
          }

          /*def in_detail(multisim: String, in_detail: String, debut: String, fin: String): DataFrame = {

              spark.sql(
                s"""
                  |SELECT a.msisdn, b.montant, b.marche, b.day
                  |FROM $multisim a
                  |LEFT JOIN $in_detail b
                  |where b.day BETWEEN "$debut" AND "$fin"
                """.stripMargin)

              /*val df1= spark.sql(
                s"""
                  |SELECT
                  |     msisdn,
                  |     AVG(montant) AS moy_montant_recharge,
                  |     marche,
                  |     day,
                  |     count(*) AS nombre_recharge
                  |FROM $table
                  |WHERE day BETWEEN '$debut' AND '$fin'
                  |GROUP BY msisdn, marche, day
                """.stripMargin)*/


          }*/


        def rech_in_detail(in_detail: String, debut: String, fin: String): DataFrame = {

          spark.sql(s"""
                       |WITH
                       |RankedMarkets AS (
                       |  SELECT
                       |    msisdn,
                       |    marche,
                       |    COUNT(*) as frequency,
                       |    RANK() OVER (PARTITION BY msisdn ORDER BY COUNT(*) DESC) as rank
                       |  FROM $in_detail
                       |  WHERE day BETWEEN "$debut" AND "$fin"
                       |  GROUP BY msisdn, marche
                       |),
                       |
                       |amount AS  (
                       |  SELECT
                       |      msisdn,
                       |      SUM(montant) AS montant_recharge
                       |  FROM $in_detail
                       |  WHERE day BETWEEN "$debut" AND "$fin"
                       |  GROUP BY msisdn
                       |),
                       |
                       |recence AS (
                       |  SELECT
                       |     msisdn,
                       |     MAX(day) AS last_recharge_day
                       |   FROM $in_detail
                       |   WHERE day BETWEEN "$debut" AND "$fin"
                       |   GROUP BY msisdn
                       |),
                       |
                       |frequence_recharge AS (
                       |  SELECT
                       |      msisdn,
                       |      count(*) as frequence_recharge
                       |   FROM $in_detail
                       |  GROUP BY msisdn
                       |)
                       |
                       |SELECT a.msisdn, a.montant_recharge, b.marche, c.last_recharge_day, d.frequence_recharge
                       |FROM amount a
                       |LEFT JOIN RankedMarkets b
                       |ON a.msisdn = b.msisdn AND b.rank = 1
                       |LEFT JOIN recence c
                       |ON b.msisdn = c.msisdn
                       |LEFT JOIN frequence_recharge d
                       |ON c.msisdn = d.msisdn
             """.stripMargin)
        }


          def sico(table: String): DataFrame = {
            spark.sql(
              s"""
                |SELECT nd, date_debut_engagement from $table
              """.stripMargin)
          }

          def recharge_multisim(anonym: DataFrame, multisim: DataFrame, in_detail: DataFrame,  terminaux: DataFrame, sico: DataFrame): DataFrame = {
            anonym
              .join(multisim, anonym("msisdn") === multisim("msisdn"), "left")
              .join(in_detail, multisim("msisdn") === in_detail("msisdn"), "left")
              .join(terminaux, in_detail("msisdn") === terminaux("msisdn"), "left")
              .join(sico, terminaux("msisdn") === sico("nd"), "left")
              .select(
                anonym("msisdn_h").alias("msisdn"),
                sha2(multisim("multi_sim"), 256).alias("multi_sim"),
                multisim("age"),
                multisim("sex"),
                multisim("segment_marche"),
                multisim("appel_orange_3_last_m"),
                multisim("appel_free_3_last_m"),
                multisim("appel_expresso_3_last_m"),
                multisim("min_appel_orange_3_last_m"),
                multisim("min_appel_free_3_last_m"),
                multisim("min_appel_expresso_3_last_m"),
                multisim("ca_cr_commune_day"),
                multisim("ca_cr_commune_night"),
                multisim("usage_data_90j"),
                multisim("top_appel"),
                in_detail("montant_recharge").as("total_montant_recharge"),
                in_detail("marche"),
                in_detail("last_recharge_day"),
                in_detail("frequence_recharge"),
                terminaux("type_sim"),
                sico("date_debut_engagement")
              )
          }


          def kibaru_minirun(table_case: String, table_connection: String, table_phonecal: String) : DataFrame = {
            spark.sql(
              s"""
                |SELECT
                |     I.TicketNumber,
                |     I.kib_MotifName,
                |     I.SubjectIdName,
                |     I.kib_TypeOffreName,
                |     I.kib_CategorieName,
                |     I.kib_verbatimclientName,
                |     I.kib_solutionapporteeName,
                |     I.ProductIdName,
                |     I.IncidentId,
                |     I.Title,
                |     I.kib_numromobile,
                |     I.kib_Univers,
                |     I.kib_numappelant,
                |     I.kib_fragilite,
                |     I.kib_categorieclient,
                |     I.kib_motiffragilite,
                |     I.kib_Commentairedelasolutionapporte,
                |     I.kib_nombrederetiration,
                |     I.kib_caseorigineName,
                |     P.kib_dureedetraitement,
                |     P.kib_ClientappelantName,
                |     C.Name,
                |     C.Record2Id,
                |     C.createdon,
                |     C.record1id,
                |     P.kib_groupedetraitementdelagentname,
                |     P.subject,
                |     P.actualend,
                |     P.activityid,
                |     P.phonenumber,
                |     P.modifiedon,
                |     P.description,
                |     P.actualdurationminutes,
                |     P.kib_clientobjetdeappel,
                |     P.kib_dureeattenteclient,
                |     P.kib_humeur,
                |     P.isbilled,
                |     P.kib_genesyscallduration,
                |     P.kib_languename,
                |     P.actualstart,
                |     P.kib_humeurname,
                |     I.year,
                |     I.month,
                |     I.day
                |FROM $table_case AS I
                |JOIN $table_connection AS C ON I.IncidentId = C.Record1Id
                |JOIN $table_phonecal AS P ON C.Record2Id = P.ActivityId
              """.stripMargin)
          }


          def anonym_kibaru(table: String): DataFrame = {
            val df = spark.sql(
              s"""
                 |SELECT DISTINCT phonenumber FROM $table
              """.stripMargin)

            df.withColumn("phonenumber_h", monotonically_increasing_id() + 1)
          }


        def kibaru_finale(kibaru: String, anonym: String): DataFrame = {
          spark.sql(s"""
                       |SELECT
                       |    CONCAT(
                       |        SUBSTRING(a.TicketNumber, 1, INSTR(a.TicketNumber, '-') - 1), -- Partie avant le premier tiret
                       |        '-********-', -- Masquage avec tirets de séparation
                       |        SUBSTRING(
                       |            a.TicketNumber,
                       |            INSTR(SUBSTRING(a.TicketNumber, INSTR(a.TicketNumber, '-') + 1), '-') + INSTR(a.TicketNumber, '-') + 1
                       |        ) -- Partie après le deuxième tiret
                       |    ) AS TicketNumber,
                       |    a.kib_MotifName,
                       |    a.SubjectIdName,
                       |    a.kib_TypeOffreName,
                       |    a.kib_CategorieName,
                       |    a.kib_verbatimclientName,
                       |    a.kib_solutionapporteeName,
                       |    a.ProductIdName,
                       |    a.IncidentId,
                       |    a.Title,
                       |    CONCAT('*****', SUBSTRING(a.kib_numromobile, 6)) AS kib_numromobile, -- Masquage des 5 premiers chiffres
                       |    a.kib_Univers,
                       |    CONCAT('*****', SUBSTRING(a.kib_numappelant, 6)) AS kib_numappelant, -- Masquage des 5 premiers chiffres
                       |    a.kib_fragilite,
                       |    a.kib_categorieclient,
                       |    a.kib_motiffragilite,
                       |    a.kib_Commentairedelasolutionapporte,
                       |    a.kib_nombrederetiration,
                       |    a.kib_caseorigineName,
                       |    a.kib_dureedetraitement,
                       |    CONCAT(
                       |        SUBSTRING(a.Name, 1, INSTR(a.Name, ' ') - 1),
                       |        ' ********'
                       |    ) AS Name,
                       |    a.Record2Id,
                       |    a.createdon,
                       |    a.record1id,
                       |    a.kib_groupedetraitementdelagentname,
                       |    CONCAT(
                       |        SUBSTRING(a.subject, 1, INSTR(a.subject, ' ') - 1),
                       |        ' ********'
                       |    ) AS subject,
                       |    a.actualend,
                       |    a.activityid,
                       |    CAST(b.phonenumber_h AS STRING) AS phonenumber,
                       |    a.modifiedon,
                       |    a.actualdurationminutes,
                       |    a.kib_clientobjetdeappel,
                       |    a.kib_dureeattenteclient,
                       |    a.kib_humeur,
                       |    a.isbilled,
                       |    a.kib_genesyscallduration,
                       |    a.kib_languename,
                       |    a.actualstart,
                       |    a.kib_humeurname,
                       |    a.year,
                       |    a.month,
                       |    a.day
                       |FROM $kibaru a LEFT JOIN $anonym b
                       |ON a.phonenumber = b.phonenumber
        """.stripMargin)
        }

        def kibaru_join(kibaru: String, clients: String, sico: String, anonym: String, date: String): DataFrame = {

          spark.sql(
            s"""
              |WITH
              |kib_j AS
              |(
              |SELECT
              |     a.phonenumber,
              |     a.kib_MotifName,
              |     a.kib_groupedetraitementdelagentname,
              |     a.kib_genesyscallduration,
              |     a.kib_humeurname,
              |     b.commune_arrondissement_90j as commune_arrondissement,
              |     b.ca_cr_commune_90j as ca_cr_commune,
              |     b.region_30j as region,
              |     b.age,
              |     b.segment_recharge,
              |     b.segment_marche,
              |     c.date_debut_engagement
              |FROM $kibaru a LEFT JOIN $clients b
              |ON REPLACE(a.phonenumber, '.0', '') = b.msisdn AND b.day = '$date'
              |INNER JOIN $sico c
              |ON b.msisdn = c.nd
              |)
              |
              |SELECT
              |     b.phonenumber_h AS phonenumber,
              |     a.kib_MotifName,
              |     a.kib_groupedetraitementdelagentname,
              |     a.kib_genesyscallduration,
              |     a.kib_humeurname,
              |     a.commune_arrondissement,
              |     a.ca_cr_commune,
              |     a.region,
              |     a.age,
              |     a.segment_recharge,
              |     a.segment_marche,
              |     a.date_debut_engagement
              |FROM kib_j a LEFT JOIN $anonym b
              |ON a.phonenumber = b.phonenumber
            """.stripMargin)
          /*spark.sql(
            s"""
              |WITH
              |table_clients AS (
              |   SELECT
              |         msisdn,
              |         commune_arrondissement_90j as commune_arrondissement,
              |         ca_cr_commune_90j as ca_cr_commune,
              |         region_30j as region,
              |         age,
              |         segment_recharge,
              |         segment_marche
              |   FROM $clients
                  WHERE day = '$date'
              |),
              |
            """.stripMargin)*/
        }

        def dailyClients(clients: String, anonym: String, date: String): DataFrame = {
          spark.sql(
            s"""
              |SELECT
              |     b.phonenumber_h AS phonenumber,
              |  	  a.commune_arrondissement_90j as commune_arrondissement,
              |     a.ca_cr_commune_90j as ca_cr_commune,
              |     a.region_30j as region,
              |     a.age as age,
              |     a.segment_recharge as segment_recharge,
              |     a.segment_marche as segment_marche,
              |	    a.sex as sex,
              |     a.formule_dmgp as formule_dmgp
              |FROM $clients a INNER JOIN $anonym b
              |ON a.msisdn = b.phonenumber and a.day = '$date'
              |
            """.stripMargin)
        }

        def parc_actif_4g(anonym: String, parc: String, year: String, month: String) : DataFrame = {
          spark.sql(
            s"""
              |SELECT CAST(b.msisdn_h AS STRING) AS msisdn, a.parc_actif_4g AS parc_actif_4g, a.year AS year, a.month AS month
              |FROM $parc a
              |INNER JOIN $anonym b
              |ON a.num_ligne = b.msisdn AND a.year = '2024' AND a.month = '04'
            """.stripMargin)
        }


        def deplafonnementOm(anonym: String, deplafonnement: String, year: String, month: String): DataFrame = {
          spark.sql(
            s"""
              |SELECT CAST(b.msisdn_h AS STRING) AS msisdn, a.user_grade_name AS user_grade_name
              |FROM $deplafonnement a
              |INNER JOIN $anonym b
              |ON a.msisdn = b.msisdn
              |AND a.year = "$year" AND a.month = "$month" AND a.account_status IN ('Y', 'S')
            """.stripMargin)
        }


        // Define the mask_hash UDF
        val maskHashUDF = udf((input: String) => {
          // Implement your hash function here
          input.hashCode.toString
        })

        // Define the mask_show_last_n UDF
        val maskShowLastNUDF = udf((input: String, n: Int) => {
          if (input != null && input.length > n) {
            "*" * (input.length - n) + input.takeRight(n)
          } else {
            input
          }
        })

        // Register the UDFs
        spark.udf.register("mask_hash", maskHashUDF)
        spark.udf.register("mask_show_last_n", maskShowLastNUDF)


        def export_multisim(export: String, multisim: String, in_detail: String, terminaux: String, sico: String, debut: String, fin: String): DataFrame = {
          spark.sql(
            s"""
              |WITH
              |RankedMarkets AS (
              |  SELECT
              |    msisdn,
              |    marche,
              |    COUNT(*) as frequency,
              |    RANK() OVER (PARTITION BY msisdn ORDER BY COUNT(*) DESC) as rank
              |  FROM $in_detail
              |  WHERE day BETWEEN "$debut" AND "$fin"
              |  GROUP BY msisdn, marche
              |),
              |
              |amount AS  (
              |  SELECT
              |      msisdn,
              |      SUM(montant) AS montant_recharge,
              |      MAX(day)     AS last_recharge_day,
              |      count(*) as frequence_recharge
              |  FROM $in_detail
              |  WHERE day BETWEEN "$debut" AND "$fin"
              |  GROUP BY msisdn
              |)
              |
              |
              |
              |SELECT
              |       a.msisdn                                                    ,
              |       a.numero_piece                                              ,
              |       a.anciennete                                                ,
              |       a.age                                                       ,
              |       a.equipment_identity                                        ,
              |       a.model                                                     ,
              |       a.hds_multiple_sim_support                                  ,
              |       a.device                                                    ,
              |       a.hds_platform                                              ,
              |       a.formule                                                   ,
              |       a.last_call                                                 ,
              |       a.nb_sim_orange                                             ,
              |       a.multisim_concurrent                                       ,
              |       b.sex                                                       ,
              |       b.segment_marche                                            ,
              |       b.appel_orange_3_last_m                                     ,
              |       b.appel_free_3_last_m                                       ,
              |       b.appel_expresso_3_last_m                                   ,
              |       b.min_appel_orange_3_last_m                                 ,
              |       b.min_appel_free_3_last_m                                   ,
              |       b.min_appel_expresso_3_last_m                               ,
              |       b.ca_cr_commune_day                                         ,
              |       b.ca_cr_commune_night                                       ,
              |       b.usage_data_90j                                            ,
              |       b.top_appel                                                 ,
              |       c.montant_recharge AS total_montant_recharge                ,
              |       d.marche                                                    ,
              |       c.last_recharge_day                                         ,
              |       c.frequence_recharge                                        ,
              |       e.date_debut_engagement
              |FROM       $export           a
              |LEFT JOIN  $multisim         b
              |ON a.msisdn = b.msisdn
              |LEFT JOIN  amount            c
              |ON b.msisdn = c.msisdn
              |LEFT JOIN  RankedMarkets     d
              |ON c.msisdn = d.msisdn
              |LEFT JOIN  $sico             e
              |ON d.msisdn = g.nd
                    """.stripMargin)
        }

  /*def export_multisim(export: String): DataFrame = {
    spark.sql(
      s"""
         |SELECT
          mask_hash(a.msisdn) AS msisdn                               ,
       mask_show_last_n(a.numero_piece, 3) AS numero_piece         ,
       a.anciennete                                                ,
       a.age                                                       ,
       mask_show_last_n(a.equipment_identity, 5) AS equipment_identity,
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
         |FROM       $export
         |LIMIT 20
                    """.stripMargin)
  }*/


}







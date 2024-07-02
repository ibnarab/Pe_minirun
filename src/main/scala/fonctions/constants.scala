package fonctions

import org.apache.spark.sql.SparkSession

object constants {

        val spark =

          SparkSession.builder

          .appName("multisim")

          .config("spark.hadoop.fs.defaultFS", "hdfs://bigdata")

          .config("spark.master", "yarn")

          .config("spark.submit.deployMode", "cluster")

          .enableHiveSupport()

          .getOrCreate()


        val base_multisim                     =    "refined_reporting.multisim"
        val base_recharge_in_detail           =    "refined_recharge.recharge_in_detail"
        val base_terminaux                    =    "refined_trafic.terminaux"
        val base_sico                         =    "trusted_sicli.sico"
        val base_clients                      =    "refined_vue360.daily_clients"
        val base_case                         =    "trusted_pfs.kibaru_case"
        val base_connection                   =    "trusted_pfs.kibaru_connection"
        val base_phonecall                    =    "trusted_pfs.kibaru_phonecall"
        val base_4g                           =    "refined_om.parc_actif_4g"
        val base_deplafonement                =    "trusted_om.subscribers_full"
        val base_anonym                       =    "analytics.sampleNBANBO_PREPAID_10"
        val base_export_multisim              =    "analytics.export_multisim_ext"

        val table_trafic_voix_sms             =    "refined_trafic.trafic_voix_sms"

        val table_daily_clients               =    "refined_vue360.daily_clients"

        val table_location_daytime            =    "refined_localisation.location_daytime"

        val table_location_nighttime          =    "refined_localisation.location_nighttime"

        val table_trafic_data                 =    "refined_trafic.trafic_data"



        val anonym                            =     "refined_reporting.anonym_multisim"
        val table_anonym_kibaru               =     "trusted_pfs.anonym_kibaru"
        val multisim_write                    =     "refined_reporting.recharge_multisim"
        val recharge_in_detail                =     "refined_recharge.in_detail_sim"
        val kibaru                            =     "trusted_pfs.kibaru"
        val kibaru_fin                        =     "trusted_pfs.kibaru_min"
        val kibaru_jo                         =     "trusted_pfs.kibaru_jo"
        val write_actif_4g                    =     "refined_om.parc_actif_4g_minirun"
        val write_deplafonnement              =     "refined_om.reporting_vente_deplafonnement_om_minirun"
        val write_clients                     =     "trusted_pfs.daily_clients_pe"
        val write_export_multisim             =     "analytics.export_multisim_pe"


        val mode_overwrite                    =     "overwrite"

}

import fonctions.utils._
import fonctions.constants._
import fonctions.read_write._
import fonctions.multi_sim._

object IngestionsPe {

  def main(args: Array[String]): Unit = {


    val debut = args(0)
    val fin = args(1)
    val action = args(2)




    /*if (action == "anonymisation"){
      val df = anonymisation(base_multisim)
      writeHive(df, true, "gzip", anonym)
    }*/
    if (action == "anonymisation") {
      val df = anonym_kibaru(kibaru)
      writeHive(df, true, "gzip", table_anonym_kibaru)
    }
    else if (action == "write_multisim") {
      val df_anonym = anonymisation(anonym)
      val df_multisim = multi_sim(base_multisim)
      val df_in_detail = rech_in_detail(base_recharge_in_detail, debut, fin)
      val df_terminaux = terminaux(base_terminaux, fin)
      val df_sico = sico(base_sico)
      val df_final = recharge_multisim(df_anonym, df_multisim, df_in_detail, df_terminaux, df_sico)
      writeHive(df_final, true, "gzip", multisim_write)
    } else if (action == "kibaru") {
      val df_kibaru = kibaru_minirun(base_case, base_connection, base_phonecall)
      writeHive(df_kibaru, true, "gzip", kibaru)
      //df_kibaru.printSchema()
      //df_kibaru.show(10, false)

    } else if (action == "kibaru_min") {
      val df_kibaru = kibaru_finale(kibaru, table_anonym_kibaru)
      /*df_kibaru.printSchema()
      df_kibaru.show(10, false)*/
      writeHive(df_kibaru, true, "gzip", kibaru_fin)
    } else if (action == "kibaru_join") {
      val df_kibaru = kibaru_join(kibaru, base_clients, base_sico, table_anonym_kibaru, "20240331")
      writeHive(df_kibaru, true, "gzip", kibaru_jo)

    } else if(action == "parc_actif_4g"){
      val df_4g = parc_actif_4g(base_anonym, base_4g, "2024", "04")
      writeHive(df_4g, true, "gzip", write_actif_4g)
    } else if(action == "deplafonnement_om"){
      val deplafonnement = deplafonnementOm(base_anonym, base_deplafonement, "2024", "03")
      writeHive(deplafonnement, true, "gzip", write_deplafonnement)
    } else if(action == "daily_clients_pe") {
      val df_clients = dailyClients(base_clients, table_anonym_kibaru, "20240515")
      writeHive(df_clients, true, "gzip", write_clients)
    }
    else if (action == "export_multisim") {

      val export_multisim_df      = exportMultisim    (base_export_multisim)

      val trafic_voix_sms_df      = traficVoixSms     (table_trafic_voix_sms       , debut   ,   fin)

      val daily_clients_df        = dailyClients2     (table_daily_clients         ,             fin)

      val location_daytime_df     = locationDaytime   (table_location_daytime      , debut   ,   fin)

      val location_nightime_df    = locationNighttime (table_location_nighttime    , debut   ,   fin)

      val usage_data_df           = usageData         (table_trafic_data           , debut   ,   fin)

      val top_appel_df            = topAppel          (table_trafic_voix_sms       , debut   ,   fin)


      val df_final = tableExportMultisim(export_multisim_df, trafic_voix_sms_df, daily_clients_df, location_daytime_df, location_nightime_df, usage_data_df, top_appel_df, base_recharge_in_detail, base_sico, debut, fin)
      println(df_final.count())
      writeHive(df_final, true, "gzip", write_export_multisim)
    }

  }
}

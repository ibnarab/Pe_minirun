package fonctions

import org.apache.spark.sql.{DataFrame, SaveMode}


object read_write {

        def writeHive(dataFrame: DataFrame, header: Boolean, compression: String, table: String) : Unit =  {

          dataFrame

              .repartition(1)

              .write

              .mode(SaveMode.Overwrite)

              //.partitionBy("year", "month")

              .option("header", header)

              .option("compression", compression)

              //.option("path", path)

              .saveAsTable(table)

        }


          def writeMinirun(dataFrame: DataFrame, header: Boolean, compression: String, table: String) : Unit =  {

            dataFrame

              .repartition(1)

              .write

              .mode(SaveMode.Overwrite)

              //.partitionBy("year", "month")

              .option("header", header)

              .option("compression", compression)

              //.option("path", path)

              .saveAsTable(table)

          }



}

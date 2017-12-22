package idCardDataPreprocess

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}



object Id_card_data_preprocess {

  case class UserAgeSex(imei: String, uid: Int, first_id_number: String, second_id_number: String, birthday: String, age: Int, sex: String)
  case class Imei_sex(imei: String, sex: String)

  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf()

    System.setProperty("user.name", "mzsip")
    System.setProperty("HADOOP_USER_NAME", "mzsip")
    sparkConf.setAppName("YF_ALGO_ID_CARD_DATA_PREPROCESS")
    val sc: SparkContext = new SparkContext(sparkConf)
    sc.hadoopConfiguration.set("mapred.output.compress", "false")

    // Initial Hive
    val hiveContext = new HiveContext(sc)
    hiveContext.setConf("mapred.output.compress", "false")
    hiveContext.setConf("hive.exec.compress.output", "false")
    hiveContext.setConf("mapreduce.output.fileoutputformat.compress", "false")
    println("================== Initial HIVE Done =========================")



    import hiveContext.implicits._

    // val today = "20170917"
    val today = args(0)
    val year: Int = today.substring(0,4).trim.toInt
    val month: Int = today.substring(4,6).trim.toInt
    val day: Int = today.substring(6,8).trim.toInt
    val calendar: Calendar = Calendar.getInstance

    calendar.set(year,month-1,day)
    val yestoday_Date: String = new SimpleDateFormat("yyyyMMdd").format(calendar.getTime)

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)

    // uid to imei info
    val udi_imei_table_name = "user_profile.edl_device_uid_mz_rel"
    // id card info table
    val userCerificationInfoTable: String = "user_center.ods_uc_certification_info_c"
    // get the gender and age info
    val userAgeLabel: RDD[(String, Int, String, String, String, Int, String)] = getAgeLabelRDD(hiveContext, userCerificationInfoTable, udi_imei_table_name, yestoday_Date, year)
    val userAgeLabelFine: RDD[(String, Int, String, String, String, Int, String)] = userAgeLabel.filter(v => v._6 >= 7).filter(v => v._6 <= 76)
    println("\n\n *************** The number of valid id data (age in [7, 76]): " + userAgeLabelFine.count() + " *********** \n\n")

    // count the male and female
    val maleNum = userAgeLabelFine.filter(v => v._7 == "male").count()
    val femaleNum = userAgeLabelFine.filter(v => v._7 == "female").count()
    println("================== Counting Gender Number =========================")
    println("============== Male: " + maleNum + " Female: " + femaleNum + " Gender Total: " + (maleNum + femaleNum))
    println("============== Male : Female  " + maleNum * 1.0 / femaleNum)


    // save the userAgeLabel to hive
    val userAgeLabelDF: DataFrame = userAgeLabelFine.map(v => UserAgeSex(v._1, v._2, v._3, v._4, v._5, v._6, v._7)).toDF()
    val userAgeLabelTable: String = "algo.yf_age_gender_accord_IDCard_v2"
    val userAgeLabelTableTemp: String = "age_gender_result"
    userAgeLabelDF.registerTempTable(userAgeLabelTableTemp)
    val creatUserAgeLabelSQL: String = "create table if not exists " + userAgeLabelTable + " (imei string, uid bigint, first_id_number string, second_id_number string, birthday string, age int, gender string) partitioned by (stat_date string) stored as textfile"
    val InsertUserAgeLabelSQL:String = "insert overwrite table " + userAgeLabelTable + " partition(stat_date = " + yestoday_Date + ") select * from " + userAgeLabelTableTemp
    // val creatUserAgeLabelSQL: String = "create table if not exists " + userAgeLabelTable + " (user_id bigint, age_range string, sex string) stored as textfile"
    // val InsertUserAgeLabelSQL:String = "insert overwrite table " + userAgeLabelTable + " select * from " + userAgeLabelTableTemp
    hiveContext.sql(creatUserAgeLabelSQL)
    hiveContext.sql(InsertUserAgeLabelSQL)

    val flyme_gender_table_name: String = "algo.yf_imei_sex_for_build_sexmodel_flyme"
    extractOnlyInIdCardData(flyme_gender_table_name, hiveContext, userAgeLabelFine)
  }

  // function to get the data samples only in idcard data, not be included by dataset used to build sex model
  def extractOnlyInIdCardData(flyme_data_for_build_sex_model: String,
                              hiveContext: HiveContext,
                              id_card_data_rdd: RDD[(String, Int, String, String, String, Int, String)]
                             ) = {
    // flyme gneder info
    import hiveContext.implicits._
    val select_flyme_gender_sql: String = "select * from " + flyme_data_for_build_sex_model
    val flyme_gender_rdd: RDD[(String, Int)] = hiveContext.sql(select_flyme_gender_sql).rdd.map(v => (v.getString(0), v.getInt(1)))
    val data_only_in_idcard: RDD[(String, Int)] = id_card_data_rdd.map(v => {
      val gender: String = v._7
      var gender_int: Int = -1
      if (v._7 == "male")
        gender_int = 1
      if (v._7 == "female")
        gender_int = 0
      (v._1, gender_int)
    }).subtractByKey(flyme_gender_rdd)
    println("\n\n ************* id_card_data_count:" + id_card_data_rdd.count() +
      " only_in_id_card_count: " + data_only_in_idcard.count() + " ********* \n\n")

    // insert only_in_idcard_rdd to table alog.yf_sex_known_data_only_in_idcard
    val only_in_idcard_df: DataFrame = data_only_in_idcard.map(v => {
      var sex_str: String = ""
      if (v._2 == 1)
        sex_str = "male"
      else
        sex_str = "female"
      (v._1, sex_str)
    }).map(v => Imei_sex(v._1, v._2)).toDF()

    only_in_idcard_df.registerTempTable("temp_table")
    val only_in_idcard_df_creat_sql: String = "create table if not exists algo.yf_sex_label_known_and_only_in_idcard (imei string, sex string) stored as textfile"
    val only_in_idcard_df_insert_sql: String = "insert overwrite table algo.yf_sex_label_known_and_only_in_idcard select * from temp_table"
    hiveContext.sql(only_in_idcard_df_creat_sql)
    hiveContext.sql(only_in_idcard_df_insert_sql)
    println("\n\n ************************* Extract data only in id_card and Insert table (Done) *************************** \n\n")

  }

  // function to get age_label_rdd
  // return: (imei:String, uid:Int, first_id_number:String, second_id_number:String, birthday:String, user_age:Int, gender:String)
  def getAgeLabelRDD(
                      hiveContext: HiveContext,
                      userCerificationInfoTable: String,
                      uid_imei_table_name: String,
                      yestoday_Date: String,
                      currentYear: Int): RDD[(String, Int, String, String, String, Int, String)] = {

    val selectDataSQL = "SELECT user_id, first_id_number, second_id_number from " + userCerificationInfoTable  + " where first_id_number REGEXP '^[0-9].*$'"
    // (user_id, first_id_number, second_id_number)
    // eg: (616	37110219850124	3)
    val resDF: DataFrame = hiveContext.sql(selectDataSQL)
    val resDFUniqByIdCard_by_uid: DataFrame = resDF.dropDuplicates(Seq("first_id_number", "second_id_number"))
    println("\n\n ************************** Raw_ID_data_number: " + resDF.count() + "***********************\n\n")
    println("\n\n ************************** Uniq_ID_data_number: " + resDFUniqByIdCard_by_uid.count() + "*************** \n\n")
    val uid_imei_select_sql: String = "select imei, uid from " + uid_imei_table_name + " where stat_date=" + yestoday_Date
    val uid_imei_DF: DataFrame = hiveContext.sql(uid_imei_select_sql)
    // format: (imei, uid, first_id_number, second_id_number)
    val resDFUniqByIdCard: DataFrame = resDFUniqByIdCard_by_uid.join(uid_imei_DF, resDFUniqByIdCard_by_uid("user_id") === uid_imei_DF("uid")).select("imei", "uid", "first_id_number", "second_id_number")

    // parse the result data frame
    // format: (imei:String, uid:Int, first_id_number:String, second_id_number:String, birthday:String, user_age:Int, gender:String)
    val rddUserLabel: RDD[(String, Int, String, String, String, Int, String)] = resDFUniqByIdCard.map(
      r => {
        val imei: String = r.get(0).toString.trim
        val uid: Int = r.get(1).toString.trim.toInt

        var ageLabel: Int = 0
        var gender: String = null

        // get the age label of user
        val first_id_number: String = r.get(2).toString.trim
        val second_id_number: Int = r.get(3).toString.trim.toInt
        val birthday: String = first_id_number.substring(6, 14)
        val birthYear: Int = birthday.substring(0, 4).toInt
        val user_age = currentYear - birthYear

        // get the gender label of user
        if (second_id_number % 2 == 0)
          gender = "female"
        else
          gender = "male"

        (imei:String, uid:Int, first_id_number:String, second_id_number.toString:String, birthday:String, user_age:Int, gender:String)
      }
    )
    rddUserLabel
  }
}









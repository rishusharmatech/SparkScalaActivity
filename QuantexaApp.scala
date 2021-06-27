package rishu.quantexaactivity.spark.solution

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SaveMode, SparkSession}

object QuantexaApp extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    if (args.length == 0) {

      logger.error(" Usage: QuantexaApp FlightFileName:String PassengerFileName:String NumberOfFlightsTogether[optional]:Int From[optional]:String To[optional]:String ")
      logger.error(""" e.g : QuantexaApp /Users/flightData.csv /Users/passengers.csv 5 "2021-05-21" "2021-05-21" """)
      System.exit(1)

    }

    logger.info("Starting Application")

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Quantexa-Activity")
      .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
      .config("spark.hadoop.fs.s3a.fast.upload","true")
      .config("spark.sql.parquet.filterPushdown", "true")
      .config("spark.speculation", "false")
      .enableHiveSupport()
      .getOrCreate()

    /*Setting Shuffle Partitions to 3 --> local[3] threads */
    spark.conf.set("spark.sql.shuffle.partitions", 3)

    logger.info("Spark.confs = " + spark.conf.getAll.toString())

    val FlightFilePath = args(0)
    logger.info("Flight File Name --> " + args(0))
    val PassengerFilePath = args(1)
    logger.info("Passenger File Name --> " + args(1))

    logger.info("funcRegisterInputData: Started")
    funcRegisterInputData(spark,FlightFilePath,PassengerFilePath);
    logger.info("funcRegisterInputData: Complete")

    logger.info("funcOutput1: Started")
    funcOutput1(spark)
    logger.info("funcOutput1: Complete")

    logger.info("funcOutput2: Started")
    funcOutput2(spark)
    logger.info("funcOutput2: Complete")

    logger.info("funcOutput3: Started")
    funcOutput3(spark)
    logger.info("funcOutput3: Complete")

    logger.info("funcOutput4: Started")
    funcOutput4(spark)
    logger.info("funcOutput4: Complete")

    if (args.length == 5) {
      logger.info("funcOutput5: Started")
      funcOutput5(spark, args(2).toInt, args(3).toString, args(4).toString)
      logger.info("funcOutput5: Complete")
    }

    logger.info("Finished Application")

    spark.stop()

  }

  def funcRegisterInputData(spark: SparkSession, FlightFilePath: String, PassengerFilePath: String): Unit = {

    logger.info("Reading Flight Data")
    val TempFlight_df = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(FlightFilePath)
    logger.info("Reading Flight Data Complete")
    val Flight_df = TempFlight_df.withColumn("date",col("date").cast("date"))

    logger.info("Reading Passenger Data")
    val Passenger_df = spark.read.format("csv")
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(PassengerFilePath)
    logger.info("Reading Passenger Data Complete")

    Flight_df.coalesce(numPartitions = 1).write.bucketBy(numBuckets = 3, colName = "passengerId")
      .mode(SaveMode.Overwrite)
      .saveAsTable("Flight")
    logger.info("Flight table registered with --> 3 Buckets")

    Passenger_df.coalesce(numPartitions = 1).write.bucketBy(numBuckets = 3, colName = "passengerId")
      .mode(SaveMode.Overwrite)
      .saveAsTable("Passenger")
    logger.info("Passenger table registered with --> 3 Buckets")

  }

  def funcOutput1(spark: SparkSession): Unit = {

    logger.info("OUTPUT 1 <----> Number of Flights Each Month")
    val out1 = spark.sql("""Select month(date) as month,
                            count(*) as Number_Of_Flights
                            From Flight ft
                            Group By month(date) Order By month(date)""")
    out1.show()
    out1.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("Result1")
    logger.info("OUTPUT 1 <----> Number of Flights Each Month: Saved as Result1")
  }

  def funcOutput2(spark: SparkSession): Unit = {

    logger.info("OUTPUT 2 <----> Names Of The 100 Most Frequent Flayers")
    val out2 = spark.sql("""Select Tab1.passengerId,
                            Tab1.Cnt as Number_Of_Flights,
                            Tab2.firstName as First_Name,
                            Tab2.lastName as Last_Name
                            From
                            (Select passengerId, Count(*) as Cnt From Flight ft Group By passengerId) Tab1
                            Inner Join
                            Passenger Tab2
                            On
                            Tab1.passengerId = Tab2.passengerId
                            Order By Number_Of_Flights Desc limit 100
                            """)

    out2.limit(100).show(100)
    out2.limit(100).coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("Result2")
    logger.info("OUTPUT 2 <----> Names Of The 100 Most Frequent Flayers: Saved as Result2")
  }

  def funcOutput3(spark: SparkSession): Unit = {

    logger.info("OUTPUT 3 <----> Greatest Number Of Countries")
    val out3 = spark.sql("""Select Result.passengerId, Count(*) As Number_Of_Countries_Run
                     From
                     (
                     Select passengerId,from as country from Flight ft where upper(from) <> 'UK'
                     union
                     Select passengerId,to as country from Flight ft where upper(to) <> 'UK'
                     ) Result
                     group by Result.passengerId
                     Order By Number_Of_Countries_Run Desc
                     """)
    out3.show()
    out3.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("Result3")
    logger.info("OUTPUT 3 <----> Greatest Number Of Countries: Saved as Result3")

  }

  def funcOutput4(spark: SparkSession): Unit = {

    logger.info("OUTPUT 4 <----> More then 3 flights together")
    val out4 = spark.sql("""Select Passenger1Id, Passenger2Id, Count(*) as Number_Of_Flights_Together from
                        (select pass1.passengerId as Passenger1Id, pass2.passengerId as Passenger2Id, pass1.flightId as FlightId
                        from
                        (select passengerId, flightId from flight) pass1
                        inner join
                        (select passengerId, flightId from flight) pass2
                        on
                        pass1.flightId = pass2.flightId Where pass1.passengerId <> pass2.passengerId)
                        Group By Passenger1Id,Passenger2Id Having Count(*) > 3
                        """)
    out4.show()
    out4.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("Result4")
    logger.info("OUTPUT 4 <----> More then 3 flights together: Saved as Result4")
  }

  def funcOutput5(spark: SparkSession, NumberOfFlightsTogether: Int, From: String, To: String): Unit = {

    spark.sql("""Select distinct count(*) over (partition by passengerId1,passengerId2,date order by date) as cnt, passengerId1, passengerId2,date
             from
             (Select
              pass1.passengerId as passengerId1 ,
              pass2.passengerId as passengerId2,
              pass1.date as date
              from
              (select * from flight) pass1 inner join
              (select * from flight) pass2
              on
              pass1.flightId = pass2.flightId where
              pass1.passengerId <> pass2.passengerId
              )""").coalesce(1).write.bucketBy(numBuckets = 3, colName = "date").mode(SaveMode.Overwrite).saveAsTable("Flowntogether")
    logger.info("Flowntogether table registered with --> 3 Buckets")

    val out5 = spark.sql("""Select passengerId1,passengerId2,Sum(NumberOfFlightsTogether) NumberOfFlightsTogether , MIN(date) From ,MAX(date) To From
                          (
                          Select passengerId1,passengerId2,cnt as NumberOfFlightsTogether,date From flowntogether
                          where
                          date >= """ + """'""" + From.toString() + """'""" + """ and date <= """ + """'""" + To.toString() + """'""" + """
                          )
                          group by passengerId1,passengerId2 Having NumberOfFlightsTogether >""" + NumberOfFlightsTogether.toString())

    logger.info("OUTPUT 5 <----> Flown Together")
    out5.show()
    out5.coalesce(1).write.option("header", true).mode(SaveMode.Overwrite).csv("Result5")
    logger.info("OUTPUT 5 <----> Flown Together: Saved as Result5")
  }

}

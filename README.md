# SparkScalaActivity
Project: Spark/Scala activity for Quantexa

The activity loads two csv files for flight and passenger data (both csv) and performs calculations to output various parameters such as total number of flights for each month,100 most frequent flyers,greatest number of countries a passenger has been in without being in the UK, the passengers who have been on more than 3 flights together etc. and few more. 

Entire process is build using spark/scala. The output is stored in the user/appliction-user directory as discrete folders/files along with console output.

It also creates and registers bucketed tables on hive/spark metastore for intermediate processing. Bucketing has been used to optimize the join operations and enable bucketed join while joining datasets.

#Usage e.g.
spark-submit --files C:\BigData\QuantexaActivity\log4j.properties --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=C:\BigData\QuantexaActivity\log4j.properties -Dlogfile.name=Driverlog" --master local[3] C:\BigData\QuantexaActivity\quantexaactivity_2.12-0.1.jar C:\BigData\QuantexaActivity\flightData.csv C:\BigData\QuantexaActivity\passengers.csv 5 "2017-01-01" "2017-03-01"

Files Provided:

log4j.properties: log4j logger settings file.
quantexaactivity_2.12-0.1.jar : Packaged jar file.
flightData.csv : Flight input file
passengers.csv: Passenger input file.
QuantexaApp.scala: Scala code file.
build.sbt: SBT build properties and dependencies.
Outputs: All outputs.

The main program takes below parameters:
Usage: QuantexaApp FlightFileName:String PassengerFileName:String NumberOfFlightsTogether[optional]:Int From[optional]:String To[optional]:String


Functions:

def main(args: Array[String]): Main function.
def funcRegisterInputData(spark: SparkSession, FlightFilePath: String, PassengerFilePath: String): Function to register input data.
def funcOutput1(spark: SparkSession): Function to output total number of flights for each month.
def funcOutput2(spark: SparkSession): Function to output 100 Most Frequent Flayers.
def funcOutput3(spark: SparkSession): Function to output greatest number Of countries apart from UK a passenger has been.
def funcOutput4(spark: SparkSession): Function t output more then 3 flights together.
def funcOutput5(spark: SparkSession, NumberOfFlightsTogether: Int, From: String, To: String): Function to output flow together dataset based on number and date range.

Deployment : Jar file has been packaged for deployment to local or cluster mode operations.

Testing Strategy: As the entire module has been developed using fuctional strategy, hence an inedpendent test class has been coupled to test every function independently 

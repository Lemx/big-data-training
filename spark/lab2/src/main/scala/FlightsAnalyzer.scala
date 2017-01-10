import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Column, DataFrame, SQLContext}
import org.apache.spark.sql.functions._

object FlightsAnalyzer {

  def main(args: Array[String]): Unit = {
    val flightsFile = args(0)
    val carriersFile = args(1)
    val airportsFile = args(2)
    val conf = new SparkConf()
      .setAppName("lab2")
    val sc = new SparkContext(conf)

    val sql = new SQLContext(sc)

    val flights = loadDfFromCsv(flightsFile, sql)
    val carriers = loadDfFromCsv(carriersFile, sql)
    val airports = loadDfFromCsv(airportsFile, sql)

    flights.sample(withReplacement = false, 0.01).show
    carriers.sample(withReplacement = false, 0.01).show
    airports.sample(withReplacement = false, 0.01).show

    val cachedAirportsOrigin = innerJoinDFs(flights, "Origin", airports, "iata").cache
    val cachedAirportsDest = innerJoinDFs(flights, "Dest", airports, "iata").cache
    val cachedFlightsCarriers = innerJoinDFs(flights, "UniqueCarrier", carriers, "Code").cache

    groupByColumnCountOrderByCount(cachedFlightsCarriers, "Description", desc).show

    val nycOrigin = filter(cachedAirportsOrigin, Seq(("Month", 6), ("city", "New York"))).count
    val nycDest = filter(cachedAirportsDest, Seq(("Month", 6), ("city", "New York"))).count
    val nycTotal = nycOrigin + nycDest
    println(s"NYC June'07 total: $nycTotal")

    val usAirportsOrigin = filter(cachedAirportsOrigin, Seq(("Month", Seq(6,7,8)), ("country", "USA")))
    val usAirportsDest = filter(cachedAirportsDest, Seq(("Month", Seq(6,7,8)), ("country", "USA")))

    groupByColumnCountOrderByCount(usAirportsOrigin.unionAll(usAirportsDest), "airport", desc).show(5)
    groupByColumnCountOrderByCount(cachedFlightsCarriers, "Description", desc).show(1)
  }

  private def loadDfFromCsv(path: String, sql: SQLContext): DataFrame = {
    sql.read
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("parserLib", "univocity") // default parser is too picky at times
        .option("mode", "DROPMALFORMED")
        .load(path)
        .toDF
  }

  def innerJoinDFs(left: DataFrame, leftColumn: String, right: DataFrame, rightColumn: String):DataFrame = {
      left.join(right, left.col(leftColumn) === right.col(rightColumn))
  }

  def groupByColumnCountOrderByCount(dataFrame: DataFrame, column: String, directionFunc: (String) => Column): DataFrame = {
    dataFrame.groupBy(column)
                .count
                .orderBy(directionFunc("count"))
  }

  def filter(dataFrame: DataFrame, conditions: Seq[(String, Any)]): DataFrame = {
    def _filter(dataFrame: DataFrame, conditions: Seq[(String, Any)]): DataFrame = {
      conditions match {
        case (Nil) => dataFrame
        case (head :: tail) => _filter(head match {
          case (s: String, value: Seq[Any]) => dataFrame.where(dataFrame.col(s).isin(value:_*))
          case (s: String, value: Any) => dataFrame.where(dataFrame.col(s) === value)
        }, tail)
      }
    }
    _filter(dataFrame, conditions)
  }
}

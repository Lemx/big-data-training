import breeze.linalg._
import breeze.plot._
import org.apache.spark.ml.classification.{BinaryLogisticRegressionSummary, LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.jfree.chart.axis.NumberTickUnit

import scala.collection.immutable.HashSet

object ResponseAnalyzer {

  val schema = StructType(Seq(
    StructField("age", IntegerType, nullable = true),
    StructField("isEmployed", IntegerType, nullable = true),
    StructField("isRetired", IntegerType, nullable = true),
    StructField("sex", IntegerType, nullable = true),
    StructField("numberOfChildren", IntegerType, nullable = true),
    StructField("numberOfDependentPeople", IntegerType, nullable = true),
    StructField("education", IntegerType, nullable = true),
    StructField("maritalStatus", IntegerType, nullable = true),
    StructField("branchOfEmployment", IntegerType, nullable = true),
    StructField("position", IntegerType, nullable = true),
    StructField("companyOwnership", IntegerType, nullable = true),
    StructField("relationshipToForeignCapital", IntegerType, nullable = true),
    StructField("directionOfActivityInsideTheCompany", IntegerType, nullable = true),
    StructField("familyIncome", IntegerType, nullable = true),
    StructField("personalIncome", FloatType, nullable = true),
    StructField("cityOfRegistration", IntegerType, nullable = true),
    StructField("cityOfLiving", IntegerType, nullable = true),
    StructField("thePostalAddressOfTheCity", IntegerType, nullable = true),
    StructField("cityOfTheBranchWhereTheLoanWasTaken", IntegerType, nullable = true),
    StructField("state", IntegerType, nullable = true),
    StructField("registeredAddressAndAddressOfTheActualStayMatch", IntegerType, nullable = true),
    StructField("addressOfActualResidenceAndMailingAddressMatch", IntegerType, nullable = true),
    StructField("registeredAddressAndPostalAddressMatch", IntegerType, nullable = true),
    StructField("postalActualAddressRegisterMatch", IntegerType, nullable = true),
    StructField("registrationAreaTheActualResidenceMailingAddressAndLocationOfTheAreaOfTheOutletWhereTheMatchTookALoan", IntegerType, nullable = true),
    StructField("ownerOfApartment", IntegerType, nullable = true),
    StructField("numberOfCarsOwned", IntegerType, nullable = true),
    StructField("doesHeSheOwnsACarProducedDomestically", IntegerType, nullable = true),
    StructField("doesHeSheOwnsAHouseOutsideACity", IntegerType, nullable = true),
    StructField("doesHeSheOwnsACottage", IntegerType, nullable = true),
    StructField("doesHeSheOwnsAGarage", IntegerType, nullable = true),
    StructField("doesHeSheOwnsALand", IntegerType, nullable = true),
    StructField("theAmountOfTheLastLoan", FloatType, nullable = true),
    StructField("termOfALoan", IntegerType, nullable = true),
    StructField("downPayment", FloatType, nullable = true),
    StructField("isADriversLicenseIndicatedInTheQuestionnaire", IntegerType, nullable = true),
    StructField("isStatePensionFundIndivatedInTheQuestionnaries", IntegerType, nullable = true),
    StructField("numberOfMonthLivingAtTheAddressOfFactResidence", IntegerType, nullable = true),
    StructField("durationOfWorkAtTheCurrentWorkingPlaceInMonth", IntegerType, nullable = true),
    StructField("isThereAStationaryPhoneAtTheResidenceAddress", IntegerType, nullable = true),
    StructField("isThereAStationaryPhoneAtTheRegistrationAddress", IntegerType, nullable = true),
    StructField("isThereAWorkPhone", IntegerType, nullable = true),
    StructField("numberOfLoans", IntegerType, nullable = true),
    StructField("numberOfClosedLoans", IntegerType, nullable = true),
    StructField("theNumberOfPaymentsMadeByTheClient", IntegerType, nullable = true),
    StructField("theNumberOfPaymentDelaysCommittedByTheClient", IntegerType, nullable = true),
    StructField("theMaximumOrderNumberOfTheDelayCommittedByTheClient", IntegerType, nullable = true),
    StructField("averageAmountOfTheDelayedPaymentUSD", FloatType, nullable = true),
    StructField("maximumAmountOfTheDelayedPaymentUSD", FloatType, nullable = true),
    StructField("theNumberOfUtilizedCards", IntegerType, nullable = true)
  ))

  val categories: HashSet[String] = HashSet[String]("isEmployed", "isRetired", "sex", "education", "maritalStatus", "branchOfEmployment",
    "position", "companyOwnership","relationshipToForeignCapital", "directionOfActivityInsideTheCompany",
    "familyIncome", "cityOfRegistration", "cityOfLiving", "thePostalAddressOfTheCity",
    "cityOfTheBranchWhereTheLoanWasTaken", "state", "registeredAddressAndAddressOfTheActualStayMatch",
    "addressOfActualResidenceAndMailingAddressMatch", "registeredAddressAndPostalAddressMatch",
    "postalActualAddressRegisterMatch",
    "registrationAreaTheActualResidenceMailingAddressAndLocationOfTheAreaOfTheOutletWhereTheMatchTookALoan",
    "ownerOfApartment", "doesHeSheOwnsACarProducedDomestically", "doesHeSheOwnsAHouseOutsideACity",
    "doesHeSheOwnsACottage", "doesHeSheOwnsAGarage","doesHeSheOwnsALand",
    "isADriversLicenseIndicatedInTheQuestionnaire", "isStatePensionFundIndivatedInTheQuestionnaries",
    "isThereAStationaryPhoneAtTheResidenceAddress", "isThereAStationaryPhoneAtTheRegistrationAddress",
    "isThereAWorkPhone"
  )

  def main(args: Array[String]): Unit = {
    val objectsFile = args(0)
    val targetFile = args(1)
    val rocPlot = args(2)

    val conf = new SparkConf()
      .setAppName("lab2")
        .setMaster("local[*]")
    val sc = new SparkContext(conf)
    val sql = new SQLContext(sc)

    val observations = loadDfFromCsv(objectsFile, schema, sql)
    val target = loadDfFromCsv(targetFile, StructType(Seq(StructField("target", IntegerType))), sql)
    val combined: RDD[Row] = concatDataFrames(observations, target)

    val data = sql.createDataFrame(combined, StructType(observations.schema ++ target.schema))
    val completeCases = data.na.fill(Map("theNumberOfUtilizedCards" -> 0)).na.drop("any")

    val clean: DataFrame = cleanData(completeCases, sql)

    val oheMap = createOheMap(clean)
    sc.broadcast(oheMap)

    val oheEncoded = encodeDataFrameToOhe(clean, oheMap)
    val labeledData = convertRowsToLabeledPoints(oheEncoded).cache

    val summary = runRegression(labeledData, sql)

    print(s"AUC: ${summary.areaUnderROC}")

    plotRocAndSave(summary, rocPlot)
  }

  private def loadDfFromCsv(path: String, schema: StructType, sql: SQLContext): DataFrame = {
    sql.read
      .format("com.databricks.spark.csv")
      .option("header", "false")
      .option("delimiter", ";")
      .option("nullValue", "NaN")
      .schema(schema)
      .load(path)
      .toDF
  }

  private def concatDataFrames(observations: DataFrame, target: DataFrame): RDD[Row] = {
    val combined = observations.rdd
      .zipWithIndex()
      .map(x => x.swap)
      .join(target.rdd.zipWithIndex().map(x => x.swap))
      .map(x => Row.fromSeq(x._2._1.toSeq ++ x._2._2.toSeq))
    combined
  }

  private def cleanData(completeCases: DataFrame, sQLContext: SQLContext): DataFrame = {
    import sQLContext.implicits._
    val clean = completeCases.where($"age" >= 21)
      .where($"numberOfChildren" >= 0)
      .where($"numberOfDependentPeople" >= 0)
      .where($"personalIncome" >= 0)
      .where($"theAmountOfTheLastLoan" >= 0)
      .where($"termOfALoan" > 0)
      .where($"downPayment" >= 0)
      .where($"numberOfMonthLivingAtTheAddressOfFactResidence" >= 0 && $"numberOfMonthLivingAtTheAddressOfFactResidence" <= $"age" * 12)
      .where($"durationOfWorkAtTheCurrentWorkingPlaceInMonth" >= 0 && $"durationOfWorkAtTheCurrentWorkingPlaceInMonth" <= ($"age" - 16) * 12)
      .where($"numberOfLoans" >= 0)
      .where($"numberOfClosedLoans" >= 0 && $"numberOfClosedLoans" <= $"numberOfLoans")
      .where($"theNumberOfPaymentsMadeByTheClient" >= 0)
      .where($"theNumberOfPaymentDelaysCommittedByTheClient" >= 0 && $"theNumberOfPaymentDelaysCommittedByTheClient" <= $"theNumberOfPaymentsMadeByTheClient")
      .where($"theMaximumOrderNumberOfTheDelayCommittedByTheClient" >= 0)
      .where($"averageAmountOfTheDelayedPaymentUSD" >= 0)
      .where($"maximumAmountOfTheDelayedPaymentUSD" >= 0 && $"maximumAmountOfTheDelayedPaymentUSD" >= $"averageAmountOfTheDelayedPaymentUSD")
      .where($"theNumberOfUtilizedCards" >= 0)
    clean
  }

  private def createOheMap(clean: DataFrame): Map[Int, Map[Any, Int]] = {
    clean.schema
      .zipWithIndex
      .filter(x => categories.contains(x._1.name))
      .map(x => x._2 -> clean.select(x._1.name)
        .distinct.rdd.collect
        .flatMap(x => x.toSeq)
        .zipWithIndex
        .map(x => x._1 -> x._2).toMap).toMap
  }

  private def encodeDataFrameToOhe(clean: DataFrame, oheMap:Map[Int, Map[Any, Int]]): RDD[Row] = {
    clean.map(x => Row.fromSeq(x.toSeq.zipWithIndex
      .map(x => if (oheMap.contains(x._2))
        encodeOhe(x._2, x._1, oheMap)
      else
        Seq(x._1))
      .reduce(_ ++ _)))
  }

  def encodeOhe(category:Int, levelValue:Any, map:Map[Int, Map[Any, Int]]): Seq[Int] = {
    val levels = map(category)
    val levelOrder = levels(levelValue)
    val totalLevels = levels.size
    levelOrder match {
      case 0 => 1 +: Seq.fill(totalLevels-1)(0)
      case i: Int => (Seq.fill(i)(0) :+ 1) ++ Seq.fill(totalLevels - i - 1)(0)
    }
  }

  private def convertRowsToLabeledPoints(oheEncoded: RDD[Row]): RDD[LabeledPoint] = {
    oheEncoded.map(x => LabeledPoint(x.getAs[Integer](x.length - 1).toDouble,
      Vectors.sparse(x.length - 1, x.toSeq
        .dropRight(1)
        .zipWithIndex
        .filter(x => x match {
          case (v: Float, _) => v != 0.0f
          case (v: Int, _) => v != 0
        })
        .map(x => x match {
          case (v: Float, i: Int) => (i, v.toDouble)
          case (v: Int, i: Int) => (i, v.toDouble)
        }))))
  }

  private def runRegression(labeledData: RDD[LabeledPoint], sQLContext: SQLContext): BinaryLogisticRegressionSummary = {
    import sQLContext.implicits._
    val lr = new LogisticRegression()
    val pipeline = new Pipeline().setStages(Array(lr))

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.elasticNetParam, Array(0, 0.03, 0.1, 0.5, 0.9, 1))
      .addGrid(lr.maxIter, Array(10, 20, 50, 100, 200))
      .addGrid(lr.regParam, Array(0.3, 0.1, 0.01))
      .build()

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator) // default metric is AUC
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(10)

    val cvModel = cv.fit(labeledData.toDF)

    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(0).asInstanceOf[LogisticRegressionModel]
    val summary = bestModel.summary.asInstanceOf[BinaryLogisticRegressionSummary]
    summary
  }

  private def plotRocAndSave(summary: BinaryLogisticRegressionSummary, rocPlot:String): Unit = {
    val f = Figure()
    val p = f.subplot(0)
    p += plot(summary.roc.map(x => x.getAs[Double](0)).collect, summary.roc.map(x => x.getAs[Double](1)).collect)
    val x = linspace(0.0, 1.0)
    p += plot(x, x :* 1.0, '.')
    p.xlabel = "false positives"
    p.ylabel = "true positives"
    p.xlim = (0.0, 1.0)
    p.xaxis.setTickUnit(new NumberTickUnit(0.1))
    p.yaxis.setTickUnit(new NumberTickUnit(0.1))
    f.refresh
    f.saveas(rocPlot)
  }
}

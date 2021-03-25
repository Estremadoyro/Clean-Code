import org.apache.spark.sql.DataFrame;
import org.apache.spark.util.SizeEstimator;
import org.apache.spark.sql.functions.desc;

object Ejercicio2 extends App {

val DataScheme: DataFrame = spark.read.parquet("processed");
class EvaluateDataFrame (val dataScheme: DataFrame,
                         val PrmSparkStatsLevel: Int,
                         val sparkPartitionId: String) {
    val dfPartitions = dataScheme.rdd.getNumPartitions
    require(PrmSparkStatsLevel > 0, """Spark level must be
    greater than 0""")
}

//Main super trait
trait PrmLevel1 {
    def toMb (num: Float): Float = num / 1024.toFloat / 1024.toFloat
    def dfStats(df: EvaluateDataFrame) = {
        val dfSizeByte: BigInt = df.dataScheme.queryExecution.optimizedPlan.stats.sizeInBytes
        val dfSizeMb: Float = toMb(dfSizeByte.toFloat)
        val dfSizeEstimatorMb: Float = toMb(SizeEstimator.estimate(df.dataScheme))
        println("*** STATS *** Df Stats PlanSizeMB:"+dfSizeMb+",  EstimatorSizeMB:"
                +dfSizeEstimatorMb+", PartitionNum: "+df.dfPartitions+", PartitionSizeMB:"
                +dfSizeMb / df.dfPartitions+", PartitionSizeMB:"+dfSizeEstimatorMb / df.dfPartitions)
    }
}

trait PrmLevel2 extends PrmLevel1 {
    override def dfStats(df: EvaluateDataFrame) = {
        super.dfStats(df);
        println(s"""*** STATS *** Df Partition record 
        count ${df.dataScheme.count()}""");
    }
}

trait PrmLevel3 extends PrmLevel1 {
    override def dfStats(df: EvaluateDataFrame) = {
        super.dfStats(df);
        println(s"*** STATS *** Df Partition Info");
        df.dataScheme
          .groupBy(df.sparkPartitionId)
          .count()
          .orderBy(desc("count"))
          .show(if (df.dfPartitions>0) df.dfPartitions else 200);
    }
}

trait PrmLevel8 extends PrmLevel1 {
    override def dfStats(df: EvaluateDataFrame) = {
        super.dfStats(df);
        println(s"*** STATS *** Show data:")
        df.dataScheme.show() 
    }
}

class PrmSparkStatsLevel1 (val df: EvaluateDataFrame) extends PrmLevel1 {
    super.dfStats(df)
}

class PrmSparkStatsLevel2 (val df: EvaluateDataFrame) extends PrmLevel2 {
    super.dfStats(df)
} 
class PrmSparkStatsLevel3 (val df: EvaluateDataFrame) extends PrmLevel3 {
    super.dfStats(df)
}

class PrmSparkStatsLevel4 (val df: EvaluateDataFrame) extends PrmLevel2 with PrmLevel3 {
    super.dfStats(df)
}

class PrmSparkStatsLevel8 (val df: EvaluateDataFrame) extends PrmLevel8 {
    super.dfStats(df)
}

val df1: EvaluateDataFrame = new EvaluateDataFrame(DataScheme, 1, "cutoff_date")
val prm1: PrmSparkStatsLevel1 = new PrmSparkStatsLevel1(df1)
}

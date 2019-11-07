package ch.epfl.lts2.wikipedia
import java.sql.Timestamp
import java.time.{Duration, LocalDate, LocalDateTime}

import breeze.linalg._
import breeze.stats._
import com.github.servicenow.ds.stats.stl.SeasonalTrendLoess

object TimeSeriesUtils {

  def getPeriodHours(startDate:LocalDate, endDate:LocalDate):Int = {
    val visitsPeriod = Duration.between(startDate.atStartOfDay, endDate.plusDays(1).atStartOfDay)
    visitsPeriod.toHours.toInt
  }
  /**
    * Converts a (possibly sparse) list of visits at given times to a vector (regularly sampled)
    * @param v list of (time, visit count) tuples
    * @param startTime time of the index 0 of resulting vector
    * @param totalHours length of time interval covered by @v
    * @return vector of visit counts
    */
  def densifyVisitList(v:List[(Timestamp, Int)], startTime:LocalDateTime, totalHours:Int):Array[Double] = {
    val vd = new VectorBuilder(v.map(p => Duration.between(startTime, p._1.toLocalDateTime).toHours.toInt).toArray,
                               v.map(_._2.toDouble).toArray, v.size, totalHours).toDenseVector
    vd.toArray
  }

  def _compareTimeSeries(v1:Array[Double], v2:Array[Double], lambda:Double = 0.5):Double = {
    val vPairs =  v1.zip(v2)
    val similarity = vPairs.map(p => scala.math.min(p._1, p._2) / scala.math.max(p._1, p._2))
                     .filter(v => !v.isNaN && v > lambda)
    if (similarity.isEmpty) 0 else similarity.sum
  }
  /**
    * Computes similarity of two time-series
    *
    * @param v1Visits   page id of edge start
    * @param v2Visits   page id of edge end
    * @param startTime time of the index 0 of resulting vector
    * @param totalHours length of time interval covered by @v
    * @param isFiltered Specifies if filtering is required (divides values by the number of spikes)
    * @param lambda Similarity threshold (discard pairs having a lower similarity)
    * @return Similarity measure
    */
  def compareTimeSeries(v1Visits:List[(Timestamp, Int)], v2Visits:List[(Timestamp, Int)],
                        startTime:LocalDateTime, totalHours:Int,
                        isFiltered: Boolean = true, lambda: Double = 0.5): Double = {

    val v1 = densifyVisitList(v1Visits, startTime, totalHours)
    val v2 = densifyVisitList(v2Visits, startTime, totalHours)
    val mFreq = if (isFiltered) totalHours else 1.0

    val v1Cnt = removeDailyVariations(v1).map(_/mFreq).map(scala.math.max(_, 0.0)) // do not allow negative visits count
    val v2Cnt = removeDailyVariations(v2).map(_/mFreq).map(scala.math.max(_, 0.0))
    _compareTimeSeries(v1Cnt, v2Cnt)
  }

  /**
    * Compute the Pearson correlation of two vectors
    * @param a first vector
    * @param b second vector
    * @return Pearson correlation in [-1;1]
    */
  def pearsonCorrelation(a: Array[Double], b: Array[Double]): Double = {
    if (a.length != b.length)
      throw new IllegalArgumentException("Vectors should have same length")
    val n = a.length

    val va = DenseVector(a)
    val vb = DenseVector(b)

    val ma = meanAndVariance(va)
    val mb = meanAndVariance(vb)

    1.0 / (n - 1.0) * sum(v = ((va - ma.mean) / ma.stdDev) *:* ((vb - mb.mean) / mb.stdDev) )
  }

  /**
    * Performs decomposition of a signal (Seasonal Loess) and
    * returns a version of the input without the daily variations
    * @param y input data, hourly sampled
    * @return Data with daily variations smoothed out
    */
  def removeDailyVariations(y: Array[Double]):Array[Double] = {
    val stb = new SeasonalTrendLoess.Builder()
    val s = stb.setPeriodLength(24)
               .setSeasonalWidth(240)
               .setRobust()
               .buildSmoother(y)
               .decompose()

    s.getTrend
  }


  /**
    * Smoothed zero-score algorithm shamelessly copied from https://stackoverflow.com/a/22640362/6029703
    * Uses a rolling mean and a rolling deviation (separate) to identify peaks in a vector
    *
    * @param y         - The input vector to analyze
    * @param lag       - The lag of the moving window (i.e. how big the window is)
    * @param threshold - The z-score at which the algorithm signals (i.e. how many standard deviations away from the moving mean a peak (or signal) is)
    * @param influence - The influence (between 0 and 1) of new signals on the mean and standard deviation (how much a peak (or signal) should affect other values near it)
    * @return - The calculated averages (avgFilter) and deviations (stdFilter), and the signals (signals)
    */
  def smoothedZScore(y: Array[Double], lag: Int, threshold: Double, influence: Double): Array[Int] = {

    // the results (peaks, 1 or -1) of our algorithm
    val signals = DenseVector.zeros[Int](y.length)

    // filter out the signals (peaks) from our original list (using influence arg)
    val filteredY = DenseVector(y)

    // the current average of the rolling window
    val avgFilter = DenseVector.zeros[Double](y.length)

    // the current standard deviation of the rolling window
    val stdFilter = DenseVector.zeros[Double](y.length)

    val v = y.take(lag)


    // init avgFilter and stdFilter
    //y.take(lag).foreach(s => stats.addValue(s))
    val m = meanAndVariance(v)
    avgFilter(lag - 1) = m.mean
    stdFilter(lag - 1) = m.stdDev // getStandardDeviation() uses sample variance (not what we want)

    // loop input starting at end of rolling window
    y.zipWithIndex.slice(lag, y.length - 1).foreach {
      case (s: Double, i: Int) =>
        // if the distance between the current value and average is enough standard deviations (threshold) away
        if (Math.abs(s - avgFilter(i - 1)) > threshold * stdFilter(i - 1)) {
          // this is a signal (i.e. peak), determine if it is a positive or negative signal
          signals(i) = if (s > avgFilter(i - 1)) 1 else -1
          // filter this signal out using influence
          filteredY(i) = (influence * s) + ((1 - influence) * filteredY(i - 1))
        } else {
          // ensure this signal remains a zero
          signals(i) = 0
          // ensure this value is not filtered
          filteredY(i) = s
        }

        // update rolling average and deviation
        val vup = DenseVector(filteredY.slice(i - lag, i).toArray)

        avgFilter(i) = mean(vup)
        stdFilter(i) = stddev(vup) // getStandardDeviation() uses sample variance (not what we want)
    }
    signals.toArray
  }
}

package ch.epfl.lts2.wikipedia
import breeze.linalg._
import breeze.stats._


object TimeSeriesUtils {
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
  def smoothedZScore(y: DenseVector[Double], lag: Int, threshold: Double, influence: Double): Array[Int] = {

    // the results (peaks, 1 or -1) of our algorithm
    val signals = DenseVector.zeros[Int](y.length)

    // filter out the signals (peaks) from our original list (using influence arg)
    val filteredY = y.copy

    // the current average of the rolling window
    val avgFilter = DenseVector.zeros[Double](y.length)

    // the current standard deviation of the rolling window
    val stdFilter = DenseVector.zeros[Double](y.length)

    val v = y.slice(0, lag - 1)


    // init avgFilter and stdFilter
    //y.take(lag).foreach(s => stats.addValue(s))
    val m = meanAndVariance(v)
    avgFilter(lag - 1) = m.mean
    stdFilter(lag - 1) = m.stdDev // getStandardDeviation() uses sample variance (not what we want)

    // loop input starting at end of rolling window
    y.data.zipWithIndex.slice(lag, y.length - 1).foreach {
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

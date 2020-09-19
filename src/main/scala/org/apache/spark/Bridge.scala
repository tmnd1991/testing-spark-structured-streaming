package org.apache.spark

import org.apache.spark.util.AccumulatorContext

object Bridge {
  def clearAccumulators(): Unit = AccumulatorContext.clear()
}

package com.test.spark.wiki.extracts

import org.apache.spark.sql.Encoders

object Implicit {
  implicit val encd1 = Encoders.product[Faits]
  implicit val encd2 = Encoders.product[FaitArtist]
  implicit val encd3 = Encoders.product[DeltaB]
  implicit val encd4 = Encoders.product[Ener]
}

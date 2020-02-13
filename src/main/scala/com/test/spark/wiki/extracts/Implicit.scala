package com.test.spark.wiki.extracts

import org.apache.spark.sql.Encoders

object Implicit {
  implicit val encd1 = Encoders.product[Faits]
  implicit val encd2 = Encoders.product[FaitArtist]
  implicit val encd3 = Encoders.product[DeltaB]
  implicit val encd4 = Encoders.product[Ener]
  implicit val encd5 = Encoders.product[RddFlux]
  implicit val encd6 = Encoders.product[NDF]
  implicit val encd7 = Encoders.product[Genre]
  implicit val encd8 = Encoders.product[Montant]
  implicit val encd9 = Encoders.product[TotaleMontantType]
  implicit val encd10 = Encoders.product[Portfolio]
  implicit val encd11 = Encoders.product[Deal]
  implicit val encd12 = Encoders.product[Edge]
}

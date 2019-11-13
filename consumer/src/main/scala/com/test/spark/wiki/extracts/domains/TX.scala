package com.test.spark.wiki.extracts.domains

import com.fasterxml.jackson.annotation.JsonProperty

case class TX(id:String, @JsonProperty("type") atype:String , @JsonProperty("date") adate:Long, account:String, amount:Double)
case class AccountType(account:String, atype:String)
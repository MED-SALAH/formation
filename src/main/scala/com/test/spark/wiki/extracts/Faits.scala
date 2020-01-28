package com.test.spark.wiki.extracts

case class Faits(id:String, Track:String, Artist:String
                 , Genre:String, BeatsPerMinute:Double
                 , Energy:Int, Danceability:Int, LoudnessdB:Int
                 , Liveness:Int, Valence:Int
                 , Length:Int, Acousticness:Int
                 , Speechiness:Int, Popularity:Int)
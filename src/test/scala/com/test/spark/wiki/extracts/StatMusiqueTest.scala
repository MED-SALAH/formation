package com.test.spark.wiki.extracts

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.scalatest.{FlatSpec, FunSuite}
import com.test.spark.wiki.extracts.Implicit._
class StatMusiqueTest extends FlatSpec {
  implicit val spark: SparkSession=SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
  "getFaitartistcount should return a count" should "OK" in {
  //Given
  //val FilePath = "src\\main\\resources\\top50.csv"
    val artist1 = Faits("1","Se�orita","Shawn Mendes","canadian pop",117,55,76,-6,8,75,191,4,3,79)
    val artist2 = Faits("26","If I Can't Have You","Shawn Mendes","canadian pop",124,82,69,-4,13,87,191,49,6,70)
    val artist3 = Faits("2","China","Anuel AA","reggaeton flow",105,81,79,-4,8,61,302,8,9,92)
    val listFaits = spark.createDataset(Seq(artist1,artist2,artist3))
    val listArtist1 = FaitArtist("Shawn Mendes",2)
    val listArtist2 = FaitArtist("Anuel AA",1)


    val expected = spark.createDataset(Seq(listArtist1,listArtist2))



  //When
    val result:Dataset[FaitArtist] = StatMusique.getFaitArtistCount(listFaits)
    expected.show()
    result.show()

  //Then
    assert(result.collect().sameElements(expected.collect()))


}
  "get top" should "ok" in {
    //Given
    val N = 3
    val colName = "Danceability"
    val fait1 = Faits("1","Se�orita","Shawn Mendes","canadian pop",117,55,76,-6,8,75,191,4,3,79)
    val fait2 = Faits("26","If I Can't Have You","Shawn Mendes","canadian pop",124,82,69,-4,13,87,191,49,6,70)
    val fait3 = Faits("2","China","Anuel AA","reggaeton flow",105,81,79,-4,8,61,302,8,9,92)
    val fait4 = Faits("3","boyfriend (with Social House)","Ariana Grande","dance pop",190,80,40,-4,16,70,186,12,46,85)
    val fait5 = Faits("4","Beautiful People (feat. Khalid)","Ed Sheeran","pop",93,65,64,-8,8,55,198,12,19,86)
    val listFaits = spark.createDataset(Seq(fait1,fait2,fait3,fait4,fait5))
//    val listEnergy1 = Energy("26","If I Can't Have You",82)
//    val listEnergy2 = Energy("2","China",81)
//    val listEnergy3 = Energy("3","boyfriend (with Social House)",80)

    val expected = spark.createDataset(Seq(fait3,fait1,fait2))

    //When

    val result:Dataset[Faits] = StatMusique.getTopByCol(listFaits,N,colName)
    expected.show()
    result.show()

    //Then
    assert(result.collect().sameElements(expected.collect()))

  }
  "delta BeatsPerMinute" should "ok" in {
    //Given
    val fait1 = Faits("1","Se�orita","Shawn Mendes","canadian pop",117,55,76,-6,8,75,191,4,3,79)
    val fait2 = Faits("26","If I Can't Have You","Shawn Mendes","canadian pop",124,82,69,-4,13,87,191,49,6,70)
    val fait3 = Faits("2","China","Anuel AA","reggaeton flow",105,81,79,-4,8,61,302,8,9,92)
    val fait4 = Faits("3","boyfriend (with Social House)","Ariana Grande","dance pop",190,80,40,-4,16,70,186,12,46,85)
    val fait5 = Faits("4","Beautiful People (feat. Khalid)","Ed Sheeran","pop",93,65,64,-8,8,55,198,12,19,86)
    val listFaits = spark.createDataset(Seq(fait1,fait2,fait3,fait4,fait5))

    val expected = spark.createDataset(Seq(fait3,fait1,fait2))

    //When
    val result:DataFrame = StatMusique.getDeltaBeats(listFaits)
    expected.show()
    result.show()

    //Then
    assert(result.collect().sameElements(expected.collect()))

  }
}

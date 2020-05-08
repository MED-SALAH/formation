package kafka

import com.datastax.spark.connector.SomeColumns
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.kafka.common.Cluster

object CassandraTest {

  def main(args: Array[String]): Unit = {

    println("starting .... ")

    val conf = new SparkConf(true).setMaster("local[*]").setAppName("cass")
      .set("spark.cassandra.connection.host","127.0.0.1")
      .set("spark.cassandra.auth.username", "cassandra")
      .set("spark.cassandra.auth.password", "cassandra"); //cassandra host

    val sc = new SparkContext(conf)


//    val cluster = Cluster.builder().addContactPoint(cassandra_host).build()
//    val sc = cluster.connect()
//    session.execute("CREATE KEYSPACE IF NOT EXISTS TransactionKS WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
//    session.execute("CREATE TABLE IF NOT EXISTS TransactionKS.transactionTB (id_transaction text, message text,  PRIMARY KEY(id_transaction)) ")
//    session.close()

    CassandraConnector(conf).withSessionDo { sc =>
      sc.execute("CREATE KEYSPACE test2 WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      sc.execute("CREATE TABLE test2.words (word text PRIMARY KEY, count int)")
    }


    val rdd = sc.cassandraTable("test", "tabtest")
    println(rdd)
    println(rdd.count)
    println(rdd.first)


    val col = sc.parallelize(Seq(("key3", 3),("key4", 4)))
    println("colllllllllllll",col)
    col.saveToCassandra("test", "tabtest", SomeColumns("topic", "nbr"))
  }

}


import java.util.Properties
object HelloWorld {
   /* This is my first java program.  
   * This will print 'Hello World' as the output
   */
   def main(args: Array[String]) {
   	  println("*****************************************************************") // prints Hello World4

      println("****************  BDA Assignment 2 *****************************") // prints Hello World4

      println("********************************** *****************************") // prints Hello World4

	  println("******************  Query 1a *************************************")
      val url = "jdbc:postgresql://localhost:5432/BDA?user=postgres&password=admin"
      val connectionProperties = new Properties()
      connectionProperties.setProperty("Driver", "org.postgresql.Driver")
      val query1a = "(select date_of_event,count(*) as opened_per_day from  pull_request  where events='opened' group by date_of_event order by date_of_event) as t"
      val query1df = spark.read.jdbc(url, query1a, connectionProperties)
      query1df.show()

      println("################################################################") // prints Hello World4

      println("******************  Query 1_b *************************************")
      val query1b = "(select date_of_event,count(*) as opened_per_day from  pull_request where events='discussed' group by date_of_event order by date_of_event ) as t"
      val query1dfb = spark.read.jdbc(url, query1b, connectionProperties)
      query1dfb.show()
      println("################################################################")

      
      println("******************  Query 2 *************************************")
      val query2 = "(select Distinct on(mm) name_of_author from(select* from (select name_of_author, max(ee) as ee1,mm from (select name_of_author, count(events) as ee,date_trunc('month', date_of_event) as mm from pull_request where events='discussed' group by mm,name_of_author) as tempp group by name_of_author,mm order by mm DESC) as tempp3 group by mm,name_of_author,ee1 order by ee1 DESC) as dd order by mm,ee1 DESC) as temmmp"
      val query2df = spark.read.jdbc(url, query2, connectionProperties)
      query2df.show()
      println("################################################################")

      println("******************  Query 3 *************************************")
      val query3 = "(select Distinct on(mm) name_of_author from(select* from (select name_of_author, max(ee) as ee1,mm from (select name_of_author, count(events) as ee,date_trunc('week', date_of_event) as mm from pull_request where events='discussed' group by mm,name_of_author) as tempp group by name_of_author,mm order by mm DESC) as tempp3 group by mm,name_of_author,ee1 order by ee1 DESC) as dd order by mm,ee1 DESC) as temmp"
      val query3df = spark.read.jdbc(url, query3, connectionProperties)
      query3df.show()
      println("################################################################")

      println("******************  Query 4 *************************************")
      val query4 = "(select count(*),date_trunc('week', date_of_event) from pull_request where events = 'opened' group by date_trunc('week', date_of_event) order by date_trunc desc ) as temmp"
      val query4df = spark.read.jdbc(url, query4, connectionProperties)
      query4df.show()
      println("################################################################")


      println("******************  Query 5 *************************************")
      val query5 = "(select sum(res),Extract(month from date_of_event) as month_in_2010 from(select Count(*) as res,date_of_event from pull_request where Extract(year from date_of_event)=2010 and events='merged' group by date_of_event) as xx group by Extract(month from date_of_event)) as temmp"
      val query5df = spark.read.jdbc(url, query5, connectionProperties)
      query5df.show()
      println("################################################################")


      println("******************  Query 6 *************************************")
      val query6 = "(select count(events),date_of_event from pull_request group by EXTRACT(day from date_of_event ),date_of_event order by date_of_event) as temmp"
      val query6df = spark.read.jdbc(url, query6, connectionProperties)
      query6df.show()
      println("################################################################")

      println("******************  Query 7 *************************************")
      val query7 = "(select name_of_author,pull_request_from_author_in_2011 from(select name_of_author,count(events) as pull_request_from_author_in_2011 from pull_request where events='opened' and EXTRACT(year from date_of_event )='2011' group by name_of_author) as dd order by pull_request_from_author_in_2011 DESC Limit 1) as temmp"
      val query7df = spark.read.jdbc(url, query7, connectionProperties)
      query7df.show()
      println("################################################################")
   }
}
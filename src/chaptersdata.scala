import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger



object chaptersdata extends App {
  
      // show logs if error 
    Logger.getLogger("org").setLevel(Level.ERROR)
      //Spark context creation here local cluster * 
    val sc = new SparkContext("local[*]","errorlogs")
    //base rdd chapter data
    val chapterDataRDD =  sc.textFile("C:/Users/ROG STRIX/Desktop/trendytech/week10/ASS/chapters.csv").map(x =>(x.split(",")(0).toInt,x.split(",")(1).toInt))
    //base rdd for title
    val titleDataRDD =  sc.textFile("C:/Users/ROG STRIX/Desktop/trendytech/week10/ASS/titles.csv").map(x =>(x.split(",")(0).toInt,x.split(",")(1)))
    
    //ex:1 chapter count in  course
    val chapterCountRDD = chapterDataRDD.map(x => (x._2,1)).reduceByKey((x,y) => x+y)
    //base rdd view data
    val viewDataRDD = sc.textFile("C:/Users/ROG STRIX/Desktop/trendytech/week10/ASS/views*.csv").map(x =>(x.split(",")(1).toInt,x.split(",")(0).toInt))
    val viewDataDistinctRDD = viewDataRDD.distinct()
    val joinedRDD = viewDataDistinctRDD.join(chapterDataRDD)
    val viewCountRDD = joinedRDD.map(x => (x._2,1))
    val totalViewsRDD = viewCountRDD.reduceByKey((x,y)=> x+y)
    val droppedIDCount = totalViewsRDD.map(x => (x._1._2,x._2))
    val newJoinedRDD = droppedIDCount.join(chapterCountRDD)
    val percentView = newJoinedRDD.mapValues(x => x._1.toDouble/x._2)
    val formattedPercent = percentView.mapValues(x => f"$x%01.5f".toDouble)
    val scoreRDD = formattedPercent.mapValues(x => {
      
      if(x >= 0.9) 10
      else if(x >= 0.5 && x < 0.9) 4
      else if(x >= 0.25 && x < 0.5) 2
      else 0
    }).reduceByKey(_+_)
    val titleJoinRDD = scoreRDD.join(titleDataRDD)
    val finalRDD = titleJoinRDD.map(x => (x._2._1,x._2._2)).sortByKey(false).collect.foreach(println)
    
    
    
    
    }
    
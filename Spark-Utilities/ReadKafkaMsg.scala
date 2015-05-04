val sc = new SparkConf()
      .setAppName(appName)
      .set("spark.executor.memory", "1024m")
      .set("spark.cores.max", "3")
      .set("spark.app.name", appName)
      .set("spark.ui.port", sparkUIPort)

 val ssc =  new StreamingContext(sc, Milliseconds(emitInterval.toInt))

KafkaUtils
      .createStream(ssc, zookeeperQuorum, consumerGroup, topicMap)
      .map(_._2)
      .foreachRDD( (rdd:RDD, time: Time) => {
        println("Time %s: (%s total records)".format(time, rdd.count()))
      }
package com.myx.gmall.realtime.utils

import java.util

import com.myx.gmall.realtime.bean.{DauInfo, Movie}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, DocumentResult, Get, Index, Search, SearchResult}
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, TermQueryBuilder}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder
import org.elasticsearch.search.sort.SortOrder

/**
 * @Description: 操作ES的客户端工具类
 * @author: mayx
 * @date: 2021/11/24 16:28
 */
object MyESUtil {
  // 声明Jest客户端工厂
  private var jestFactory: JestClientFactory = null

  // 提供获取Jest客户端的方法
  def getJestClient: JestClient = {
    if (jestFactory == null) {
      // 创建Jest客户端工厂对象
      build()
    }
    jestFactory.getObject
  }

  def build(): Unit = {
    jestFactory = new JestClientFactory
    jestFactory.setHttpClientConfig(new HttpClientConfig
        .Builder("http://hadoop201:9200")
        .multiThreaded(true)
        .maxTotalConnection(20)
        .connTimeout(10000)
        .readTimeout(1000).build())
  }

  //向ES中插入单条数据  方式1  将插入文档的数组以json的形式直接传递
  def putIndex1: Unit = {
    // 获取客户端连接
    val jestClient: JestClient = getJestClient
    // 定义执行的source
    var source: String =
      """
        |{
        |  "id":300,
        |  "name": "operation meigong river",
        |  "doubanScore":"8.0",
        |  "actorList":[
        |    {"id":1,"name":"zhang han yu"}
        |    ]
        |}
        |""".stripMargin
    // 创建插入类 Index  Builder中的参数表示要插入到索引中的文档，底层会转换Json格式的字符串，所以也可以将文档封装为样例类对象
    var index: Index = new Index.Builder(source)
        .index("movie_index")
        .`type`("movie")
        .id("1")
        .build()

    // 通过客户端对象操作ES     execute参数为Action类型，Index是Action接口的实现类
    jestClient.execute(index)
    // 关闭连接
    jestClient.close()
  }

  //向ES中插入单条数据  方式2   将向插入的文档封装为一个样例类对象
  def putIndex2: Unit = {
    // 获取客户端连接
    var jestClient: JestClient = getJestClient
    val actorList: util.ArrayList[util.Map[String, Any]] =
          new util.ArrayList[util.Map[String, Any]]()
    val actorMap: util.HashMap[String, Any] = new util.HashMap[String, Any]()
    // 添加演员id
    actorMap.put("id",1)
    // 添加演员名字
    actorMap.put("name","铁蛋神猴")
    actorList.add(actorMap)

    // 封装样例类
    val movie: Movie = new Movie(400,"天下第一",8.0f,actorList)
    // 创建Action实现类
    var index: Index = new Index.Builder(movie)
        .index("movie_index")
        .`type`("movie")
        .id("4")
        .build()
    // 通过客户端对象操作ES
    jestClient.execute(index)
    // 关闭客户端连接
    jestClient.close()
  }

  //根据文档的id，从ES中查询出一条记录
  def queryIndexById: Unit = {
    // 获取客户端连接对象
    val jestClient: JestClient = getJestClient
    val get: Get = new Get.Builder("movie_index", "1").build()
    val result: DocumentResult = jestClient.execute(get)
    println(result.getJsonString)
    // 关闭连接
    jestClient.close()
  }

  //根据指定查询条件，从ES中查询多个文档  方式1
  def queryIndexByCondition1(): Unit = {
    // 获取客户端连接
    val jestClient: JestClient = getJestClient

    var query: String =
        """
        |{
        |  "query": {
        |    "bool": {
        |      "must": [
        |        {"match": {
        |          "name": "天下第一"
        |        }}
        |      ],
        |      "filter": [
        |        {"term":{"actorList.name.keyword":"铁蛋神猴"}}
        |        ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 10,
        |  "sort": [
        |    {
        |      "doubanScore": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "highlight": {
        |    "fields": {
        |      "name": {}
        |    }
        |  }
        |}
        |""".stripMargin
    // 封装Search对象
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index")
      .build()
    val result: SearchResult = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    // 将java的List转化为json的List
    import scala.collection.JavaConverters._
    val list1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
    println(list1.mkString("\n"))
    // 关闭连接
    jestClient.close()
  }
  //根据指定查询条件，从ES中查询多个文档  方式2
  def queryIndexByCondition2():Unit = {
    // 获取客户端连接
    val jestClient: JestClient = getJestClient
    //SearchSourceBuilder用于构建查询的json格式字符串
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder
    val boolQueryBuilder: BoolQueryBuilder = new BoolQueryBuilder
    boolQueryBuilder.must(new MatchQueryBuilder("name","天下第一"))
    boolQueryBuilder.filter(new TermQueryBuilder("actorList.name.keyword",
      "铁蛋神猴"))
    searchSourceBuilder.query(boolQueryBuilder)
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(10)
    searchSourceBuilder.sort("doubanScore",SortOrder.DESC)
    searchSourceBuilder.highlighter(new HighlightBuilder().field("name"))
    val query: String = searchSourceBuilder.toString
    // 封装Search对象
    val search: Search = new Search.Builder(query)
      .addIndex("movie_index")
      .build()
    val result: SearchResult = jestClient.execute(search)
    val list: util.List[SearchResult#Hit[util.Map[String, Any], Void]] = result.getHits(classOf[util.Map[String, Any]])
    // 将java的List转化为Json的List
    import scala.collection.JavaConverters._
    val list1: List[util.Map[String, Any]] = list.asScala.map(_.source).toList
    println(list1.mkString("\n"))
    // 关闭连接
    jestClient.close()
  }

  /**
   * 向ES中批量插入数据
   * @param infoList
   * @param indexName
   */
  def bulkInsert(infoList: List[(String,Any)], indexName: String): Unit = {
    if (infoList != null && infoList.size > 0) {
      // 获取客户端连接
      val jestClient: JestClient = getJestClient
      // 构建批量操作
      val bulkBuilder: Bulk.Builder = new Bulk.Builder
      for ((id,dauInfo) <- infoList) {
        val index: Index = new Index.Builder(dauInfo)
          .index(indexName)
          .`type`("_doc")
          .id(id)
          .build()
        // 将每条数据添加到批量操作中
        bulkBuilder.addAction(index)
      }
      //Bulk是Action的实现类，主要实现批量操作
      val bulk: Bulk = bulkBuilder.build()
      // 执行批量操作，获取执行结果
      val result: BulkResult = getJestClient.execute(bulk)
      //通过执行结果  获取批量插入的数据
      val items: util.List[BulkResult#BulkResultItem] = result.getItems
      println("保存到ES: " + items.size() + "条数")
      // 关闭连接
      jestClient.close()
    }

  }
  def main(args: Array[String]): Unit = {
    queryIndexByCondition2()
  }
}

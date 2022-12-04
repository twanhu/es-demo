import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.http.HttpHost
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.elasticsearch.common.text.Text
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, QueryBuilders, RangeQueryBuilder, TermQueryBuilder}
import org.elasticsearch.index.reindex.UpdateByQueryRequest
import org.elasticsearch.script.{Script, ScriptType}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.aggregations.bucket.terms.{ParsedTerms, Terms, TermsAggregationBuilder}
import org.elasticsearch.search.aggregations.metrics.{AvgAggregationBuilder, ParsedAvg}
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder

import java.util

/**
 * ES客户端,ES存储数据的格式为JSON字符串
 */
object EsTest {
  def main(args: Array[String]): Unit = {
//        println(client)
//        put()
//        post()
//        bulk()
//        updater()
//        updateByQuery()
//        delete()
//        getById()
        searchByFilter()
    //    searchByAggs()
    close()

  }

  /**
   * 查询 - 单条查询
   */
  def getById(): Unit = {
    val getRequest: GetRequest = new GetRequest("movie_test", "1005")
    val getResponse = client.get(getRequest, RequestOptions.DEFAULT)
    //获取数据（_source）
    val dataStr: String = getResponse.getSourceAsString
    println(dataStr)
  }

  /**
   * 查询 - 条件查询，先写DSL，按照 ES 的 DSL 格式写
   * search :
   * 查询doubanScore>=5.0 关键词搜索red sea
   * 关键词高亮显示
   * 显示第一页，每页2条
   * 按doubanScore从大到小排序
   */
  def searchByFilter(): Unit = {
    //SearchRequest():创建搜索请求对象
    val searchRequest: SearchRequest = new SearchRequest("movie_index")

    //SearchSourceBuilder():对搜索请求进行基本参数设置
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    //query
    //bool
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    //filter
    val rangeQueryBuilder: RangeQueryBuilder = QueryBuilders.rangeQuery("doubanScore").gte(5.0)
    boolQueryBuilder.filter(rangeQueryBuilder)
    //must
    val matchQueryBuilder: MatchQueryBuilder = QueryBuilders.matchQuery("name", "red sea")
    boolQueryBuilder.must(matchQueryBuilder)

    //query():设置此请求的搜索查询
    searchSourceBuilder.query(boolQueryBuilder)

    //分页，当前页码数=(0+1)，每页显示一条数据
    searchSourceBuilder.from(0)
    searchSourceBuilder.size(1)
    //排序
    searchSourceBuilder.sort("doubanScore", SortOrder.DESC)

    //高亮
    val highlightBuilder: HighlightBuilder = new HighlightBuilder()
    highlightBuilder.field("name")

    searchSourceBuilder.highlighter(highlightBuilder)

    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

    //获取总条数据
    val totalDocs: Long = searchResponse.getHits.getTotalHits.value

    //获取明细数据
    val hits: Array[SearchHit] = searchResponse.getHits.getHits
    for (hit <- hits) {
      //提取数据（document:文档）
      val dataJson: String = hit.getSourceAsString
      //提取高亮
      val highlightFields: util.Map[String, HighlightField] = hit.getHighlightFields
      val highlightField: HighlightField = highlightFields.get("name")
      val fragments: Array[Text] = highlightField.getFragments
      val highLightValue: String = fragments(0).toString

      println("数据：" + dataJson)
      println("高亮：" + highLightValue)
    }
  }

  /**
   * 查询 - 聚合查询，先写DSL，按照 ES 的 DSL 格式写
   *
   * 查询每位演员参演的电影的平均分，倒叙排序
   */
  def searchByAggs(): Unit = {
    //SearchRequest():创建搜索请求对象
    val searchRequest: SearchRequest = new SearchRequest("movie_index")

    //SearchSourceBuilder():对搜索请求进行基本参数设置
    val searchSourceBuilder: SearchSourceBuilder = new SearchSourceBuilder()
    //不要明细
    searchSourceBuilder.size(0)
    //group
    val termsAggregationBuilder: TermsAggregationBuilder = AggregationBuilders.terms("groupbyactorname")
      //设置用于此聚合的字段
      .field("actorList.name.keyword")
      //返回3条数据
      .size(3)
      //降序排序
      .order(BucketOrder.aggregation("doubanscoreavg", false))
    //avg：使用给定名称创建新的平均聚合
    val avgAggregationBuilder: AvgAggregationBuilder = AggregationBuilders.avg("doubanscoreavg").field("doubanScore")

    termsAggregationBuilder.subAggregation(avgAggregationBuilder)

    searchSourceBuilder.aggregation(termsAggregationBuilder)

    searchRequest.source(searchSourceBuilder)
    val searchResponse: SearchResponse = client.search(searchRequest, RequestOptions.DEFAULT)

    val aggregations: Aggregations = searchResponse.getAggregations
    val groupbyactornameParsedTerms: ParsedTerms = aggregations.get[ParsedTerms]("groupbyactorname")
    val buckets: util.List[_ <: Terms.Bucket] = groupbyactornameParsedTerms.getBuckets

    import scala.collection.JavaConverters._
    for (bucket <- buckets.asScala) {
      //演员名字
      val actorName: String = bucket.getKeyAsString
      //电影个数
      val moviecount: Long = bucket.getDocCount
      //平均分，getAggregations：获取此bucket的子聚合
      val aggregations: Aggregations = bucket.getAggregations
      val doubanscoreavgParsedAvg: ParsedAvg = aggregations.get[ParsedAvg]("doubanscoreavg")
      val avgScore: Double = doubanscoreavgParsedAvg.getValue

      println(s"$actorName 共参演了 $moviecount 部电影， 平均分为 $avgScore")
    }
  }

  /**
   * 删除
   */
  def delete(): Unit = {
    val deleteRequest: DeleteRequest = new DeleteRequest("movie_test", "oSEf5YMBbkfsuithKTWc")
    //删除document（文档）
    client.delete(deleteRequest, RequestOptions.DEFAULT)
  }

  /**
   * 修改（更新） - 单条修改，先写DSL，按照 ES 的 DSL 格式写
   */
  def updater(): Unit = {
    val updateRequest: UpdateRequest = new UpdateRequest("movie_test", "1")
    updateRequest.doc("movie_name", "功夫")
    //更新（修改）document（文档）
    client.update(updateRequest, RequestOptions.DEFAULT)
  }

  /**
   * 修改 - 条件修改，先写DSL，按照 ES 的 DSL 格式写
   */
  def updateByQuery(): Unit = {
    val updateByQueryRequest: UpdateByQueryRequest = new UpdateByQueryRequest("movie_test")

    //query
    //boolQuery(): 匹配文档并与其他查询的布尔组合匹配的查询
    val boolQueryBuilder: BoolQueryBuilder = QueryBuilders.boolQuery()
    val termQueryBuilder: TermQueryBuilder = QueryBuilders.termQuery("movie_name.keyword", "但幸福来敲门")
    boolQueryBuilder.filter(termQueryBuilder)

    updateByQueryRequest.setQuery(boolQueryBuilder)


    //update
    val params: util.HashMap[String, AnyRef] = new util.HashMap[String, AnyRef]()
    params.put("newName", "当幸福来敲门")
    val script: Script = new Script(
      ScriptType.INLINE,
      Script.DEFAULT_SCRIPT_LANG,
      "ctx._source['movie_name']=params.newName",
      params
    )
    updateByQueryRequest.setScript(script)

    //按查询请求执行更新（修改）
    client.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT)
  }

  /**
   * 批量写入
   */
  def bulk(): Unit = {
    //BulkRequest对象可以用来在一次请求中，执行多个索引、更新或删除操作
    val bulkRequest: BulkRequest = new BulkRequest()
    val movies: List[Movie] = List[Movie](
      Movie("1002", "长津湖"),
      Movie("1003", "跨过鸭绿江"),
      Movie("1004", "海棠依旧"),
      Movie("1005", "外交风云")
    )
    for (movie <- movies) {
      //指定索引
      val indexRequest: IndexRequest = new IndexRequest("movie_test")
      //将Bean对象转换成JSON字符串
      val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
      //指定doc
      indexRequest.source(movieJson, XContentType.JSON)
      //幂等写指定_id,非幂等不指定_id
      indexRequest.id(movie.id)
      //将indexRequest加入到bulk
      bulkRequest.add(indexRequest)
    }
    //将数据写入ES，批量写入
    client.bulk(bulkRequest, RequestOptions.DEFAULT)
  }

  /**
   * 单条写入 - 幂等 - 指定_id
   */
  def put(): Unit = {
    //指定索引，添加新文档 : IndexRequest
    val indexRequest: IndexRequest = new IndexRequest("movie_test")

    val movie: Movie = Movie("1001", "西游记")
    //将Bean对象转换成JSON字符串
    val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
    //指定doc(文档)
    indexRequest.source(movieJson, XContentType.JSON)
    //指定docid
    indexRequest.id("1001")

    //将数据写入ES
    client.index(indexRequest, RequestOptions.DEFAULT)
  }

  /**
   * 单条写入 - 非幂等 -不需要指定_id
   */
  def post(): Unit = {
    //指定索引，添加新文档 : IndexRequest
    val indexRequest: IndexRequest = new IndexRequest("movie_test")

    val movie: Movie = Movie("1001", "西游记")
    //将Bean对象转换成JSON字符串
    val movieJson: String = JSON.toJSONString(movie, new SerializeConfig(true))
    //指定doc(文档)
    indexRequest.source(movieJson, XContentType.JSON)

    //将数据写入ES
    client.index(indexRequest, RequestOptions.DEFAULT)
  }

  /**
   * 客户端对象
   */
  var client: RestHighLevelClient = create()

  /**
   * 创建客户端对象
   */
  def create(): RestHighLevelClient = {
    //创建RestClient连接池
    val restClientBuilder: RestClientBuilder = RestClient.builder(new HttpHost("master", 9200))

    //创建ES客户端对象
    val client: RestHighLevelClient = new RestHighLevelClient(restClientBuilder)
    client
  }

  /**
   * 关闭客户端对象
   */
  def close(): Unit = {
    if (client != null) client.close()
  }

}

case class Movie(id: String, movie_name: String)

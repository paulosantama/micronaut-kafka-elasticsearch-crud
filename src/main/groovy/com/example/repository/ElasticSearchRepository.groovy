package com.example.repository

import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.ObjectMapper
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import jakarta.inject.Inject
import jakarta.inject.Singleton
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.MatchQueryBuilder
import org.elasticsearch.index.query.Operator
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util.stream.Collectors

@Slf4j
@Singleton
@CompileStatic
class ElasticSearchRepository<I, T> {

	@Inject
	private RestHighLevelClient esClient

	@Inject
	private ObjectMapper objectMapper

	def save(String index, I id, T document) {
		try {
			IndexRequest indexRequest = new IndexRequest(index)
					.id(id.toString())
					.setRefreshPolicy('true')
					.source(objectMapper.writeValueAsString(document), XContentType.JSON)

			IndexResponse response = esClient.index(indexRequest, RequestOptions.DEFAULT)
			return response
		} catch (JsonProcessingException ex) {
			throw new RuntimeException('Falha ao serializar documento', ex)
		} catch (IOException ex) {
			throw new RuntimeException('Falha ao indexar documento', ex)
		}
	}

	Optional<T> get(String index, Class<T> clazz, I id) {
		Optional<T> maybeResponse = Optional.empty()
		try {
			GetRequest getRequest = new GetRequest(index, id.toString())
			GetResponse response = esClient.get(getRequest, RequestOptions.DEFAULT)
			T responseTyped = objectMapper.readValue(response.sourceAsString, clazz)
			maybeResponse = Optional.ofNullable(responseTyped)
		} catch (IOException ex) {
			throw new RuntimeException('Falha ao obter documento', ex)
		} finally {
			return maybeResponse
		}
	}

	def delete(String index, I id) {
		try {
			DeleteRequest deleteRequest = new DeleteRequest(index, id.toString())
			DeleteResponse response = esClient.delete(deleteRequest, RequestOptions.DEFAULT)
			return response
		} catch (IOException ex) {
			throw new RuntimeException('Falha ao deletar documento', ex)
		}
	}

	def search(String index, Class<T> clazz, String field, Object value) {
		List<T> result = []
		try {
			SearchRequest searchRequest = new SearchRequest(index)
			MatchQueryBuilder queryBuilder = QueryBuilders.matchQuery(field, value.toString()).operator(Operator.AND)
			SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
			sourceBuilder.query(queryBuilder).size(10)
			searchRequest.source(sourceBuilder)

			SearchResponse response = esClient.search(searchRequest, RequestOptions.DEFAULT)
			result = response.getHits().hits.collect().stream()
					.map { SearchHit hit -> objectMapper.convertValue(hit.sourceAsMap, clazz) }
					.collect(Collectors.toList()) as List<T>
		} catch (IOException ex) {
			throw new RuntimeException('Falha ao pesquisar documento', ex)
		} finally {
			return result
		}
	}
}

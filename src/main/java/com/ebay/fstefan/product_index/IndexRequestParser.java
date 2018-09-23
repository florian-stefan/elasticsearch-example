package com.ebay.fstefan.product_index;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;
import static org.elasticsearch.common.xcontent.XContentType.JSON;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Component;

@Component
public class IndexRequestParser {

  private static final String INDEX = "products";
  private static final String TYPE = "_doc";

  private static StatefulIndexRequestParser UNIT = new IdParser(emptyList());

  @Value("classpath:mapping.json")
  private Resource mappingResource;

  @Value("classpath:products.json")
  private Resource productsResource;

  public GetIndexRequest getGetIndexRequest() {
    return new GetIndexRequest().indices(INDEX);
  }

  public CreateIndexRequest getCreateIndexRequest() {
    try (var lines = Files.lines(Paths.get(mappingResource.getURI()))) {
      return new CreateIndexRequest(INDEX).mapping(TYPE, lines.collect(joining()), JSON);
    } catch (IOException e) {
      throw new IndexRequestParserException(e);
    }
  }

  public BulkRequest getIndexRequests() {
    try (var lines = Files.lines(Paths.get(productsResource.getURI()))) {
      BulkRequest bulkRequest = new BulkRequest();
      getIndexRequests(lines).forEach(bulkRequest::add);
      return bulkRequest;
    } catch (IOException e) {
      throw new IndexRequestParserException(e);
    }
  }

  private Stream<IndexRequest> getIndexRequests(Stream<String> lines) {
    return lines.reduce(UNIT, StatefulIndexRequestParser::parse, StatefulIndexRequestParser::union).getIndexRequests();
  }

  private abstract static class StatefulIndexRequestParser {

    private final List<IndexRequest> indexRequests;

    StatefulIndexRequestParser(List<IndexRequest> indexRequests) {
      this.indexRequests = indexRequests;
    }

    abstract StatefulIndexRequestParser parse(String line);

    StatefulIndexRequestParser union(StatefulIndexRequestParser other) {
      return this;
    }

    Stream<IndexRequest> getIndexRequests() {
      return indexRequests.stream();
    }

  }

  private static class IdParser extends StatefulIndexRequestParser {

    IdParser(List<IndexRequest> indexRequests) {
      super(indexRequests);
    }

    @Override
    StatefulIndexRequestParser parse(String line) {
      return new SourceParser(readId(line), getIndexRequests().collect(toList()));
    }

    @SuppressWarnings("unchecked")
    private String readId(String line) {
      try {
        Map<String, Map<String, String>> lineAsMap = new ObjectMapper().readValue(line, Map.class);

        return lineAsMap.get("index").get("_id");
      } catch (IOException e) {
        throw new IndexRequestParserException(e);
      }
    }

  }

  private static class SourceParser extends StatefulIndexRequestParser {

    private final String id;

    SourceParser(String id, List<IndexRequest> indexRequests) {
      super(indexRequests);

      this.id = id;
    }

    @Override
    StatefulIndexRequestParser parse(String line) {
      return new IdParser(appendRequest(new IndexRequest(INDEX, TYPE, id).source(line, JSON)));
    }

    private List<IndexRequest> appendRequest(IndexRequest indexRequest) {
      return concat(getIndexRequests(), Stream.of(indexRequest)).collect(toList());
    }

  }

  private static class IndexRequestParserException extends RuntimeException {

    IndexRequestParserException(Throwable cause) {
      super(cause);
    }

  }

}

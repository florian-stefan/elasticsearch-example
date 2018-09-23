package com.ebay.fstefan.product_index;

import static java.lang.Double.parseDouble;
import static java.lang.Math.min;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toList;
import static org.apache.lucene.search.join.ScoreMode.Avg;
import static org.elasticsearch.common.unit.Fuzziness.AUTO;
import static org.elasticsearch.search.aggregations.support.ValueType.STRING;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.List;
import java.util.StringTokenizer;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.Value;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.nested.Nested;
import org.elasticsearch.search.aggregations.bucket.nested.NestedAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequiredArgsConstructor
public class ProductSearchController {

  private static final int RESULT_PAGE_SIZE = 5;
  private static final int PRICE_RANGE_LENGTH = 25;

  private final RestHighLevelClient restHighLevelClient;

  @GetMapping
  public String getSearchForm(Model model) {
    model.addAttribute("searchRequest", new ProductSearchRequest());

    prepareFilters(model);

    return "product-list";
  }

  @GetMapping
  @RequestMapping("/products")
  public String getSearchResult(ProductSearchRequest searchRequest, Model model) {
    model.addAttribute("searchRequest", searchRequest);

    prepareFilters(model);
    searchProducts(searchRequest, model);

    return "product-list";
  }

  @GetMapping
  @RequestMapping("/products/{id}")
  public String getSearchResult(@PathVariable String id, Model model) {
    searchProduct(id, model);

    return "product-view";
  }

  private void prepareFilters(Model model) {
    SearchResponse searchResponse = executeAggregations();
    List<Status> statuses = extractStatuses(searchResponse);
    List<PriceRange> priceRanges = extractPriceRanges(searchResponse);
    List<Category> categories = extractCategories(searchResponse);

    model.addAttribute("statuses", statuses);
    model.addAttribute("priceRanges", priceRanges);
    model.addAttribute("categories", categories);
  }

  private SearchResponse executeAggregations() {
    try {
      SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
          .query(new MatchAllQueryBuilder())
          .aggregation(getStatusAggregation())
          .aggregation(getPriceAggregation())
          .aggregation(getCategoriesAggregation())
          .size(0);

      return restHighLevelClient.search(new SearchRequest("products").source(sourceBuilder));
    } catch (IOException e) {
      throw new ProductSearchException(e);
    }
  }

  private TermsAggregationBuilder getStatusAggregation() {
    return new TermsAggregationBuilder("statuses", STRING).field("status");
  }

  private RangeAggregationBuilder getPriceAggregation() {
    RangeAggregationBuilder aggregationBuilder = new RangeAggregationBuilder("price_ranges").field("price");

    for (int from = 0, to = PRICE_RANGE_LENGTH; from < 100; from += PRICE_RANGE_LENGTH, to += PRICE_RANGE_LENGTH) {
      aggregationBuilder.addRange(from, to);
    }

    return aggregationBuilder;
  }

  private NestedAggregationBuilder getCategoriesAggregation() {
    return new NestedAggregationBuilder("categories", "categories").subAggregation(getCategoriesCountAggregation());
  }

  private TermsAggregationBuilder getCategoriesCountAggregation() {
    return new TermsAggregationBuilder("category_counts", STRING).field("categories.name");
  }

  private List<Status> extractStatuses(SearchResponse searchResponse) {
    Terms statuses = searchResponse.getAggregations().get("statuses");

    return statuses.getBuckets().stream().map(Status::of).collect(toList());
  }

  private List<PriceRange> extractPriceRanges(SearchResponse searchResponse) {
    Range priceRanges = searchResponse.getAggregations().get("price_ranges");

    return priceRanges.getBuckets().stream().map(PriceRange::of).collect(toList());
  }

  private List<Category> extractCategories(SearchResponse searchResponse) {
    Nested categories = searchResponse.getAggregations().get("categories");
    Terms categoryCounts = categories.getAggregations().get("category_counts");

    return categoryCounts.getBuckets().stream().map(Category::of).collect(toList());
  }

  private void searchProducts(ProductSearchRequest searchRequest, Model model) {
    SearchResponse searchResponse = executeSearch(searchRequest);
    long totalHits = extractTotalHits(searchResponse);
    List<ProductSearchResult> productSearchResults = extractProductSearchResults(searchResponse);

    if (searchRequest.hasPreviousPage()) {
      model.addAttribute("previousPage", searchRequest.getPreviousPage());
    }
    model.addAttribute("pageInfo", searchRequest.getPageInfo(totalHits));
    if (searchRequest.hasNextPage(totalHits)) {
      model.addAttribute("nextPage", searchRequest.getNextPage());
    }
    model.addAttribute("searchResults", productSearchResults);
  }

  private SearchResponse executeSearch(ProductSearchRequest searchRequest) {
    try {
      SearchSourceBuilder sourceBuilder = new SearchSourceBuilder()
          .query(getQuery(searchRequest))
          .size(RESULT_PAGE_SIZE)
          .from(searchRequest.getFrom());

      return restHighLevelClient.search(new SearchRequest("products").source(sourceBuilder));
    } catch (IOException e) {
      throw new ProductSearchException(e);
    }
  }

  private BoolQueryBuilder getQuery(ProductSearchRequest searchRequest) {
    BoolQueryBuilder queryBuilder = new BoolQueryBuilder();

    tokenizeQueryAndAppendTokens(queryBuilder, searchRequest.getQuery());
    appendStatusFilterIfPresent(queryBuilder, deserialize(searchRequest.getStatus(), Status.class));
    appendPriceRangeFilterIfPresent(queryBuilder, deserialize(searchRequest.getPriceRange(), PriceRange.class));
    appendCategoryFilterIfPresent(queryBuilder, deserialize(searchRequest.getCategory(), Category.class));

    return queryBuilder;
  }

  private void tokenizeQueryAndAppendTokens(BoolQueryBuilder queryBuilder, String query) {
    StringTokenizer stringTokenizer = new StringTokenizer(query, " ");

    while (stringTokenizer.hasMoreTokens()) {
      String token = stringTokenizer.nextToken();
      queryBuilder.must(new MatchQueryBuilder("name", token).fuzziness(AUTO));
    }
  }

  private void appendStatusFilterIfPresent(BoolQueryBuilder queryBuilder, Status status) {
    if (status != null) {
      queryBuilder.filter(new TermQueryBuilder("status", status.getName()));
    }
  }

  private void appendPriceRangeFilterIfPresent(BoolQueryBuilder queryBuilder, PriceRange priceRange) {
    if (priceRange != null) {
      queryBuilder.filter(new RangeQueryBuilder("price").gte(priceRange.getFrom()).lte(priceRange.getTo()));
    }
  }

  private void appendCategoryFilterIfPresent(BoolQueryBuilder queryBuilder, Category category) {
    if (category != null) {
      TermQueryBuilder query = new TermQueryBuilder("categories.name", category.getName());

      queryBuilder.filter(new NestedQueryBuilder("categories", query, Avg));
    }
  }

  private long extractTotalHits(SearchResponse searchResponse) {
    return searchResponse.getHits().getTotalHits();
  }

  private List<ProductSearchResult> extractProductSearchResults(SearchResponse searchResponse) {
    return stream(searchResponse.getHits().getHits()).map(this::parseProductSearchResult).collect(toList());
  }

  private ProductSearchResult parseProductSearchResult(SearchHit searchHit) {
    return parseProductSearchResult(searchHit.getSourceAsString()).setId(searchHit.getId());
  }

  private void searchProduct(String id, Model model) {
    GetResponse getResponse = executeSearch(id);
    ProductSearchResult searchResult = extractProductSearchResult(getResponse);

    model.addAttribute("searchResult", searchResult);
  }

  private GetResponse executeSearch(String id) {
    try {
      return restHighLevelClient.get(new GetRequest("products", "_doc", id));
    } catch (IOException e) {
      throw new ProductSearchException(e);
    }
  }

  private ProductSearchResult extractProductSearchResult(GetResponse getResponse) {
    return parseProductSearchResult(getResponse.getSourceAsString()).setId(getResponse.getId());
  }

  private ProductSearchResult parseProductSearchResult(String sourceAsString) {
    try {
      return new ObjectMapper().readValue(sourceAsString, ProductSearchResult.class);
    } catch (IOException e) {
      throw new ProductSearchException(e);
    }
  }

  private static <T> T deserialize(String content, Class<T> valueType) {
    if (content == null || content.trim().isEmpty()) {
      return null;
    }

    try {
      return new ObjectMapper().readValue(content, valueType);
    } catch (IOException e) {
      throw new ProductSearchException(e);
    }
  }

  @Data
  static class ProductSearchRequest {

    private String query;
    private String status;
    private String priceRange;
    private String category;
    private Integer page;

    int getPageOrDefault() {
      return page == null ? 1 : page;
    }

    int getFrom() {
      return (getPageOrDefault() - 1) * RESULT_PAGE_SIZE;
    }

    boolean hasPreviousPage() {
      return getPageOrDefault() > 1;
    }

    int getPreviousPage() {
      return getPageOrDefault() - 1;
    }

    String getPageInfo(long totalHits) {
      int from = (getPageOrDefault() - 1) * RESULT_PAGE_SIZE + 1;
      long to = min(getPageOrDefault() * RESULT_PAGE_SIZE, totalHits);

      return "Showing results " + from + " to " + to + " of " + totalHits + ".";
    }

    boolean hasNextPage(long totalHits) {
      return getPageOrDefault() * RESULT_PAGE_SIZE < totalHits;
    }

    int getNextPage() {
      return getPageOrDefault() + 1;
    }

  }

  @Value
  static class Status extends SerializableValue {

    private String name;

    static Status of(Terms.Bucket bucket) {
      return new Status(bucket.getKeyAsString());
    }

    @JsonCreator
    static Status of(@JsonProperty("name") String name) {
      return new Status(name);
    }

  }

  @Value
  static class PriceRange extends SerializableValue {

    private double from;
    private double to;

    static PriceRange of(Range.Bucket bucket) {
      try {
        return new PriceRange(parseDouble(bucket.getFromAsString()), parseDouble(bucket.getToAsString()));
      } catch (NumberFormatException e) {
        throw new ProductSearchException(e);
      }
    }

    @JsonCreator
    static PriceRange of(@JsonProperty("from") double from, @JsonProperty("to") double to) {
      return new PriceRange(from, to);
    }

  }

  @Value
  static class Category extends SerializableValue {

    private String name;

    static Category of(Terms.Bucket bucket) {
      return new Category(bucket.getKeyAsString());
    }

    @JsonCreator
    static Category of(@JsonProperty("name") String name) {
      return new Category(name);
    }

  }

  static class SerializableValue {

    private String serialized;

    public String serialize() {
      if (serialized == null) {
        try {
          serialized = new ObjectMapper().writeValueAsString(this);
        } catch (JsonProcessingException e) {
          throw new ProductSearchException(e);
        }
      }

      return serialized;
    }

  }

  private static class ProductSearchException extends RuntimeException {

    ProductSearchException(Throwable cause) {
      super(cause);
    }

  }

}

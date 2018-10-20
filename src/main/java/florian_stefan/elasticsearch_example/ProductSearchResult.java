package florian_stefan.elasticsearch_example;

import static java.util.stream.Collectors.joining;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.util.List;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProductSearchResult {

  private String id;
  private String name;
  private String description;
  private String status;
  private double price;
  private List<Category> categories;
  private int quantity;
  private List<String> tags;

  public ProductSearchResult setId(String id) {
    this.id = id;

    return this;
  }

  public String getCategories() {
    return categories.stream().map(Category::getName).distinct().collect(joining(", "));
  }

  public String getTags() {
    return tags.stream().map(String::toUpperCase).distinct().collect(joining(", "));
  }

  @Data
  public static class Category {

    private String name;

  }

}

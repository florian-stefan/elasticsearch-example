package florian_stefan.elasticsearch_example;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Controller;

@Controller
@SpringBootApplication
public class Application {

  public static void main(String[] args) {
    SpringApplication.run(Application.class, args);
  }

  @Bean(destroyMethod = "close")
  public RestHighLevelClient restHighLevelClient() {
    return new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
  }

}

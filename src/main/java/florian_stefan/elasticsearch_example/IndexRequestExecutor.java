package florian_stefan.elasticsearch_example;

import java.io.IOException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class IndexRequestExecutor implements InitializingBean {

  private final RestHighLevelClient restHighLevelClient;
  private final IndexRequestParser indexRequestParser;

  @Override
  public void afterPropertiesSet() {
    if (isProductsIndexMissing()) {
      createProductsIndex();
      indexProductDocuments();
    }
  }

  private boolean isProductsIndexMissing() {
    try {
      return !restHighLevelClient.indices().exists(indexRequestParser.getGetIndexRequest());
    } catch (IOException e) {
      throw new IndexRequestExecutorException(e);
    }
  }

  private void createProductsIndex() {
    try {
      restHighLevelClient.indices().create(indexRequestParser.getCreateIndexRequest());
    } catch (IOException e) {
      throw new IndexRequestExecutorException(e);
    }
  }

  private void indexProductDocuments() {
    try {
      restHighLevelClient.bulk(indexRequestParser.getIndexRequests());
    } catch (IOException e) {
      throw new IndexRequestExecutorException(e);
    }
  }

  private static class IndexRequestExecutorException extends RuntimeException {

    IndexRequestExecutorException(Throwable cause) {
      super(cause);
    }

  }

}

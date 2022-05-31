package foo.v5archstudygroup.exercises.backpressure.client;

import foo.v5archstudygroup.exercises.backpressure.messages.Messages;
import foo.v5archstudygroup.exercises.backpressure.messages.converter.ProcessingRequestMessageConverter;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * This class is responsible for interacting with the REST endpoint on the server. You are allowed to change this class
 * in any way you like.
 */
public class RestClient {

    private final RestTemplate restTemplate;
    private final URI serverUri;

    public RestClient(URI serverUri) {
        var requestFactory = new SimpleClientHttpRequestFactory();
        // Always remember to set timeouts!
        requestFactory.setConnectTimeout(100);
        requestFactory.setReadTimeout(1000);
        restTemplate = new RestTemplate(List.of(new ProcessingRequestMessageConverter()));
        restTemplate.setRequestFactory(requestFactory);
        restTemplate.setErrorHandler(new BackpressureResponseErrorHandler());
        this.serverUri = serverUri;
    }

    public void sendToServer(Messages.ProcessingRequest processingRequest) {
        var uri = UriComponentsBuilder.fromUri(serverUri).path("/process").build().toUri();

        ResponseEntity<Void> booleanResponseEntity ;

        do {
            booleanResponseEntity = restTemplate.postForEntity(uri, processingRequest, Void.class);
        } while(!booleanResponseEntity.getStatusCode().equals(HttpStatus.TOO_MANY_REQUESTS));
    }

    private class BackpressureResponseErrorHandler extends DefaultResponseErrorHandler {
        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            return response.getStatusCode().equals(HttpStatus.TOO_MANY_REQUESTS) ? false : super.hasError(response) ;
        }
    }
}

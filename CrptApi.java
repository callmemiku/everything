import lombok.AccessLevel;
import lombok.Builder;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Component
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@Slf4j
public class CrptApi {

    TimeUnit timeUnit;
    Integer periodDuration; //длительность периода
    Integer limit;
    String url;
    Stack<LocalDateTime> API_CALLS = new Stack<>();
    Lock write;
    WebClient webClient;
    Long checkRate; //через какой промежуток проверяется стек

    public CrptApi(
            WebClient webClient,
            //вынести в отдельный класс конфигурационных проперти
            @Value("${app.crpt.request-limiting.time-unit}") TimeUnit timeUnit, //добавить конвертер
            @Value("${app.crpt.request-limiting.duration}") Integer periodDuration,
            @Value("${app.crpt.request-limiting.amount}") Integer limit,
            @Value("${app.crpt.api.url}") String url,
            @Value("${app.check-stack-rate-ms:1000}") Long checkRate
    ) {
        this.webClient = webClient;
        this.timeUnit = timeUnit;
        this.periodDuration = periodDuration;
        this.limit = limit;
        this.url = url;
        this.write = new ReentrantReadWriteLock(true).writeLock();
        this.checkRate = checkRate;
    }

    //удаляет записи старше duration * time unit, чтобы не было вызовов сверх нормы
    @Scheduled(
            fixedRateString = "${app.check-stack-rate-ms:1000}"
    )
    public void checkStack() throws InterruptedException {
        var delimiter = LocalDateTime.now().minus(periodDuration, timeUnit.toChronoUnit());
        while (!write.tryLock(100, TimeUnit.MILLISECONDS)) {
        }
        while (!API_CALLS.isEmpty() && API_CALLS.peek().isBefore(delimiter)) {
            API_CALLS.pop();
        }
        write.unlock();
    }

    public Mono<String> createDocument(
            Document document
    ) {
        return Mono.just(document)
                .doOnNext(doc -> callApi(doc).doOnNext(this::processAnswer).subscribe()) //запускаем отдельный реактивный поток
                .map(any -> "OK");
    }

    private Mono<ApiResponse> callApi(Document document) {
        var locked = write.tryLock();
        if (locked && API_CALLS.size() < limit) {
            API_CALLS.push(LocalDateTime.now());
            return webClient.post()
                    .uri(url)
                    .body(document, Document.class)
                    .retrieve()
                    .bodyToMono(ApiResponse.class)
                    .doOnNext(any -> write.unlock());
        } else {
            if (locked) write.unlock();
            return Mono.delay(
                    Duration.of(checkRate, ChronoUnit.MILLIS)
            ).flatMap(any -> callApi(document));
        }
    }

    public void processAnswer(ApiResponse apiResponse) {
        //любой процессинг ответа
    }

    //any object
    public static class Document {
    }

    @Builder
    //example object
    public static class ApiResponse {
        HttpStatus status;
        String message;
    }
}


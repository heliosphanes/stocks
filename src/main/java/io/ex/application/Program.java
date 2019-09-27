package io.ex.application;

import java.sql.Date;
import java.util.List;
import java.util.stream.Collectors;
import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vertx.core.json.JsonObject;
import io.vertx.rxjava.core.AbstractVerticle;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.ext.web.client.HttpResponse;
import io.vertx.rxjava.ext.web.client.WebClient;

public class Program extends AbstractVerticle {


  int counter = 0;
  static String hypothesisPath;

  static {
    hypothesisPath = System.getenv("HYPOTHESIS_PATH");
    
  }
  @Override
  public void start() {
//    ApplicationGUI gui =  new ApplicationGUI();
//    vertx.executeBlocking(b ->{
//       gui.mains();
//    }, r -> {});
//    readData();
    XSSFService service = new XSSFService();
    service.getAllCodes().forEach(System.out::println);
  }


  public Double formula(JsonObject d) {
    Double close = Double.valueOf(d.getString("4. close"));
    Double open = Double.valueOf(d.getString("1. open"));
    return ((close - open) / open) * 100;
  }

  public void readData() {

    List<String> files = vertx.fileSystem().readDirBlocking(XSSFService.path);

    List<JsonObject> allSymbolsInJsonObjects = files.parallelStream()
        .map(vertx.fileSystem()::readFileBlocking)
        .map(d -> d.toJsonObject())
        .collect(Collectors.toList());

    List<Tuple2<JsonObject,JsonObject>> timeSeriesObjects = allSymbolsInJsonObjects.stream()
        .map(object -> Tuple.of(object.getJsonObject("Meta Data"),object.getJsonObject("Time Series (Daily)")))
        .collect(Collectors.toList());

    List<Tuple2<JsonObject, List<Tuple2<String, Double>>>> data =
        timeSeriesObjects.stream()
          .map(object -> 
            Tuple.of(object._1, object._2.stream()
//                .filter()
                .filter(d -> Date.valueOf(d.getKey()).after(Date.valueOf("2018-01-01")))
                .map(d -> Tuple.of(d.getKey(), (formula((JsonObject) d.getValue()))))
                .filter(d -> Math.abs(d._2 )  > 7)
                .collect(Collectors.toList())))
          .collect(Collectors.toList());

    data.forEach(object ->{
      
      System.out.println("Symbol here : " + object._1.getString("2. Symbol") + " " + object._2.stream().count());
//      StringBuilder str  = new StringBuilder(); 
//      object._2.stream().count();
//      object._2.stream().forEach(f -> str.append(String.format("%s       %s\n", f._1, f._2)));
//      System.out.println
//      saveFile(hypothesisPath,object._1.getString("2. Symbol"),str.toString(),".txt");
    });
  }

  public void extractandStoreJsonFiles() {
    XSSFService service = new XSSFService();
    int size = service.getAllCodes().size();

    vertx.periodicStream(65000).handler(handler -> {

      System.out.println(
          "a minute has passed and the counter is: " + counter);

      service.getAllCodes().stream().skip(counter).limit(5)
          .forEach(this::scrapeDataFromSymbol);

      counter = counter + 5;

      if (counter > size)
        vertx.cancelTimer(handler.longValue());
    });
  }

  public void scrapeDataFromSymbol(String code) {

    WebClient client = WebClient.create(vertx);

    // Send a GET request
    client.get(80, "www.alphavantage.co", "/query")
        .addQueryParam("function", "TIME_SERIES_DAILY_ADJUSTED")
        .addQueryParam("symbol", code)
        .addQueryParam("outputsize", "full")
        .addQueryParam("apikey", System.getenv("KEY")).send(ar -> {
          if (ar.succeeded()) {
            // Obtain response
            HttpResponse<Buffer> response = ar.result();

            System.out.println("Received response with status code"
                + response.statusCode());
            Buffer body = response.body();

            JsonObject json = body.toJsonObject();

            System.out.println(json.encodePrettily());


            if (json.containsKey("Error Message")) {
              System.out.println(
                  "Code is not supported for outputsize full use instead compact!");
            } else {
              System.out.println("It is supported");
              saveFile(XSSFService.path,code, json.encodePrettily(),".json");
            }

          } else {
            System.out.println(
                "Something went wrong " + ar.cause().getMessage());
          }
        });

  }

  public void saveFile(String path ,String fileName, String body, String extension) {
    vertx.fileSystem().writeFile(path + fileName + extension,
        Buffer.buffer(body), result -> {
          if (result.succeeded()) {
            System.out.println(" appended");
          } else {
            System.out.println(" not appended");
          }
        });

  }
}

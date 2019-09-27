package io.ex.application;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import io.vavr.Tuple;
import io.vavr.Tuple2;

public class XSSFService {

//  String path = "/Users/helios/External/stocks-exchange-app/stocks.xlsx";
  static String path;

  static {
    path = Optional.ofNullable(System.getenv("HSTORY_PATH")).orElse(URI.create(System.getProperty("user.dir")+"/stocks.xlsx").getPath());
    
  }

  public List<List<Object>> service(String sheetId,
      String sheetName) {
    Workbook workbook = null;
    List<List<Object>> values = null;
    try {
      FileInputStream excelFile =
          new FileInputStream(new File(sheetId));

      workbook = new XSSFWorkbook(excelFile);
      Sheet datatypeSheet = workbook.getSheet(sheetName);

      Stream<Row> targetStream =
          fromIteratorToStream(datatypeSheet.iterator());

      int count = (int) fromIteratorToStream(datatypeSheet.iterator())
          .limit(1).map(r -> r.getLastCellNum()).findFirst().get();

      values = targetStream.filter(a -> a.cellIterator().hasNext()).map(currentRow -> {

        Stream<Cell> targetStream2 =
            fromIteratorToStream(currentRow.iterator());


        List<Object> rowAsList = targetStream2.map(currentCell -> {
          List<Object> arrayList1 = new ArrayList<>(
              Collections.nCopies(count , new String("")));

          CellType cellType = currentCell.getCellType();
          int columnIndex = currentCell.getColumnIndex();
          String value = "";

          switch (cellType) {
            case NUMERIC:
              value =
                  Double.toString(currentCell.getNumericCellValue());
              break;
            case BOOLEAN:
              value =
                  Boolean.toString(currentCell.getBooleanCellValue());
              break;
            case FORMULA:
              value =
                  Boolean.toString(currentCell.getBooleanCellValue());
              break;
            case BLANK:
              value = " ";
              break;
            default:
              value = currentCell.getStringCellValue();
          } arrayList1.set(columnIndex, value);
          return arrayList1;
        }).reduce((acc, first) -> {
          List<Object> collect = first.stream()
              .filter(a -> !a.toString().isEmpty()).flatMap(s -> {
                acc.set(first.indexOf(s), s);
                return acc.stream();
              }).collect(Collectors.toList());
          return collect;
        }).get();
          
        return rowAsList;

      }).collect(Collectors.toList());

    } catch (FileNotFoundException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return values;
  }

  public <T> Stream<T> fromIteratorToStream(Iterator<T> iterator) {
    Iterable<T> iterable = () -> iterator;
    Stream<T> targetStream =
        StreamSupport.stream(iterable.spliterator(), false);
    return targetStream;
  }


  public List<String> getAllCodes() {
    XSSFService service = new XSSFService();
    List<List<Object>> offlineService = service.service(path, "Software & Services");
    List<String> collect = offlineService.stream().skip(1).map(row -> row.get(1).toString()).collect(Collectors.toList());;
    return collect;
  }

  public List<Tuple2<String,String>> getCodesAndMarketCap() {
    XSSFService service = new XSSFService();
    List<List<Object>> offlineService = service.service(path, "Software & Services");
    List<Tuple2<String,String>> collect = offlineService.stream()
        .skip(1)
        .map(row -> Tuple.of(row.get(1).toString(), row.get(5).toString()))
        .collect(Collectors.toList());;
    
    
    return collect;
  }
  
}

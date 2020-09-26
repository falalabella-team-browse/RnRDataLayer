package com.hachathon.reviewNratings.service;

import com.hachathon.reviewNratings.document.ReviewsAndRatings;
import com.hachathon.reviewNratings.elasticsearch.service.ElasticsearchWrite;
import lombok.SneakyThrows;
import org.apache.commons.collections4.ListUtils;
import org.apache.poi.ss.usermodel.*;
import org.elasticsearch.action.bulk.BulkResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ReviewRatingsService {

    @Autowired
    public ElasticsearchWrite elasticsearchWrite;


    public void bulkIngest(){
        Long startTime = Instant.now().getEpochSecond();
        List<ReviewsAndRatings> reviewsAndRatingsList = new ArrayList<>();
        String inputFilePath = "/Users/shreyas-temp/Downloads/reviews_rating_data.xls";
        try{
            //reviewsAndRatingsList = parseCsv(inputFilePath);
            reviewsAndRatingsList = parseXL(inputFilePath);
            List[] subLists = partition(reviewsAndRatingsList,1000);
            List<CompletableFuture<BulkResponse>> futures = new ArrayList();
            for(int i=0;i<subLists.length;i++){
                List<ReviewsAndRatings> list = subLists[i];
                futures.add(CompletableFuture.supplyAsync(() -> elasticsearchWrite.writeBulkDocs(list)));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                    .exceptionally(ex -> null)
                    .join();
            Map<Boolean, List<CompletableFuture<BulkResponse>>> result =
                    futures.stream()
                            .collect(Collectors.partitioningBy(CompletableFuture::isCompletedExceptionally));
            Long endTimeInSec = Instant.now().getEpochSecond();

            Long elapsedTime = endTimeInSec - startTime;
            System.out.println("Elapsed time in seconds: "+elapsedTime);;
        } catch (IOException e) {

        }
    }

    private List<ReviewsAndRatings> parseCsv(String inputFilePath) throws IOException {
        File inputF = new File(inputFilePath);
        InputStream inputFS = new FileInputStream(inputF);
        BufferedReader br = new BufferedReader(new InputStreamReader(inputFS));
        // skip the header of the csv
        List<ReviewsAndRatings> reviewsAndRatingsList = br.lines().skip(1).map(mapToItem).collect(Collectors.toList());
        br.close();
        return reviewsAndRatingsList;
    }

    @SneakyThrows
    private List<ReviewsAndRatings> parseXL(String inputFilePath) throws IOException {
        List<ReviewsAndRatings> reviewsAndRatings = new ArrayList<>();
        Workbook workbook = WorkbookFactory.create(new File(inputFilePath));
        Sheet sheet = workbook.getSheetAt(0);
        Iterator<Row> rowIterator = sheet.rowIterator();
        int rowcount = sheet.getPhysicalNumberOfRows();
        for(int rowCnt=1; rowCnt < rowcount;rowCnt++){
            Row row = sheet.getRow(rowCnt);
            reviewsAndRatings.add(mapToBean(row));
        }
        workbook.close();
        return reviewsAndRatings;
    }

    private ReviewsAndRatings mapToBean(Row row){
        if(null != row && row.getLastCellNum() > 4){

            int help = getRandomNumber(1,1000);
            int sent = getRandomNumber(-100,100);
            int overall = getRandomNumber(1,10);
            int sentiment = getRandomNumber(-2,2);
            String author = null != row.getCell(0) ? String.valueOf(row.getCell(0).getNumericCellValue()):null;
            float rating = null != row.getCell(4) ? Float.valueOf(String.valueOf(row.getCell(4).getNumericCellValue())):null;
            String entityId = null != row.getCell(1)?String.valueOf(row.getCell(1).getNumericCellValue()):null;
            String title = null != row.getCell(2)?row.getCell(2).getStringCellValue():null;
            String description =null != row.getCell(2) ? row.getCell(2).getStringCellValue():null;
            return ReviewsAndRatings.builder()
                    .author(author)
                    .rating(rating)
                    .entityId(entityId)
                    .title(title)
                    .description(description)
                    .create_date(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT))
                    .modified_date(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT))
                    .helpful_count(Long.valueOf(String.valueOf(help)))
                    .sentiment_factor(Float.valueOf(String.valueOf(sent)))
                    .sentiment(sentiment)
                    .overall_rating(Float.valueOf(String.valueOf(overall)))
                    .build();
        }else{
            return null;
        }
    }

    private Function<String, ReviewsAndRatings> mapToItem = (line) -> {
        if(null != line && line.split(",").length > 4){
            String[] p = line.split(",");// a CSV has comma separated lines
            int help = getRandomNumber(1,1000);
            int sent = getRandomNumber(-100,100);
            int overall = getRandomNumber(1,10);
            int sentiment = getRandomNumber(-2,2);
            return ReviewsAndRatings.builder()
                    .author(p[0])
                    .rating(Float.valueOf(p[4]))
                    .entityId(p[1])
                    .title(p[2])
                    .description(p[3])
                    .create_date(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT))
                    .modified_date(ZonedDateTime.now().format(DateTimeFormatter.ISO_INSTANT))
                    .helpful_count(Long.valueOf(String.valueOf(help)))
                    .sentiment_factor(Float.valueOf(String.valueOf(sent)))
                    .sentiment(sentiment)
                    .overall_rating(Float.valueOf(String.valueOf(overall)))
                    .build();
        }else{
            return null;
        }
    };

    public int getRandomNumber(int min, int max) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    public static<T> List[] partition(List<T> list, int n)
    {
        // calculate number of partitions of size n each
        int m = list.size() / n;
        if (list.size() % n != 0)
            m++;

        // partition a list into sublists of size n each
        List<List<T>> itr = ListUtils.partition(list, n);

        // create m empty lists and initialize it with sublists
        List<T>[] partition = new ArrayList[m];
        for (int i = 0; i < m; i++)
            partition[i] = new ArrayList(itr.get(i));

        // return the lists
        return partition;
    }

}

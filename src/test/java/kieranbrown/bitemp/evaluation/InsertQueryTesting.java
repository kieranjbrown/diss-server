package kieranbrown.bitemp.evaluation;

import com.opencsv.CSVReader;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import io.vavr.collection.List;
import kieranbrown.bitemp.database.InsertQueryBuilder;
import kieranbrown.bitemp.database.OverlappingKeyException;
import kieranbrown.bitemp.database.QueryBuilderFactory;
import kieranbrown.bitemp.models.Trade;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.FileReader;

@DataJpaTest
@SpringJUnitConfig
class InsertQueryTesting {

    //    private final List<Integer> thresholds = List.of(100, 1000, 10000, 100000, 1000000);
    private final List<Integer> thresholds = List.of(100, 1000, 10000);
    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    private DataSource dataSource;
    private long systemTimeStart;
    private long initialMemory;
    private int objectCount;
    private Runtime runtime;
    private InsertQueryBuilder<Trade> queryBuilder;
    private List<String> results;

    @BeforeEach
    void setup() {
        entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
        systemTimeStart = 0;
        objectCount = 0;
        initialMemory = 0;
        runtime = Runtime.getRuntime();
        results = List.empty();
    }

    private java.util.stream.Stream<Trade> readObjects() throws FileNotFoundException {
        final ColumnPositionMappingStrategy<Trade> strategy = new ColumnPositionMappingStrategy<>();
        strategy.setType(Trade.class);
        strategy.setColumnMapping("buySellFlag", "id", "marketLimitFlag", "price", "stock", "systemTimeEnd", "systemTimeStart", "validTimeEnd", "validTimeStart");
        final CSVReader reader = new CSVReader(new FileReader("D:\\git\\diss-server\\InsertData.csv"));
        final CsvToBean<Trade> csvToBean = new CsvToBeanBuilder<Trade>(reader)
                .withSeparator('|')
                .withQuoteChar('\'')
                .withMappingStrategy(strategy)
                .withSkipLines(1)
                .withEscapeChar('\0')
//                .withType(Trade.class)
                .build();
        return csvToBean.stream();
    }

    @Test
    void implementationInsertQuery() throws FileNotFoundException {
        final java.util.stream.Stream<Trade> trades = readObjects();
        results = results.append("Number of Objects | Runtime (in milliseconds) | Memory Usage (in bytes)");
        systemTimeStart = System.currentTimeMillis();
        System.gc();
        initialMemory = runtime.totalMemory() - runtime.freeMemory();
        this.queryBuilder = QueryBuilderFactory.insert(Trade.class);
        trades.forEach(this::persistIndividually);
        results.forEach(System.out::println);
    }

    private void persistIndividually(final Trade trade) {
        try {
            queryBuilder.from(trade).execute(entityManager);
        } catch (final OverlappingKeyException e) {
            throw new RuntimeException("error", e);
        }
        System.out.println("objectCount:" + objectCount);
        if (thresholds.contains(++objectCount)) {
            results = results.append(String.format("%,17d | %,25d | %,23d", objectCount, System.currentTimeMillis() - systemTimeStart, (runtime.totalMemory() - runtime.freeMemory()) - initialMemory));
        }

        if (objectCount > thresholds.last()) {
            results.forEach(System.out::println);
            throw new RuntimeException("end");
        }
    }
}

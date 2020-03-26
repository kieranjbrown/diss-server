package kieranbrown.bitemp.evaluation;

import io.vavr.collection.List;
import kieranbrown.bitemp.database.InsertQueryBuilder;
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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@DataJpaTest
@SpringJUnitConfig
class InsertQueryTesting {

    private final List<Integer> thresholds = List.of(100, 1000, 10000, 100000, 1000000);
    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    private DataSource dataSource;
    private long systemTimeStart;
    private long initialMemory;
    private int objectCount;
    private Runtime runtime;
    private InsertQueryBuilder<Trade> queryBuilder;

    @BeforeEach
    void setup() {
        entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
        systemTimeStart = 0;
        objectCount = 0;
        initialMemory = 0;
        runtime = Runtime.getRuntime();
    }

    @Test
    void implementationInsertQuery() {
        final List<Trade> trades = getTrades();
        System.out.println("Number of Objects | Runtime (in milliseconds) | Memory Usage (in bytes)");
        systemTimeStart = System.currentTimeMillis();
        System.gc();
        initialMemory = runtime.totalMemory() - runtime.freeMemory();
        this.queryBuilder = QueryBuilderFactory.insert(Trade.class);
        trades.forEach(this::persist);

    }

    private void persist(final Trade trade) {
        //TODO: store in DB
//        queryBuilder.from()
        if (thresholds.contains(++objectCount)) {
            System.out.printf("%,17d | %,25d | %,23d%n", objectCount, System.currentTimeMillis() - systemTimeStart, (runtime.totalMemory() - runtime.freeMemory()) - initialMemory);
        }
    }

    private List<Trade> getTrades() {
        //TODO
        return List.ofAll(IntStream.range(1, 1000001)
                .mapToObj(x -> new Trade())
                .collect(Collectors.toList()));
    }
}

package kieranbrown.bitemp.evaluation;

import io.vavr.collection.List;
import kieranbrown.bitemp.database.QueryBuilderFactory;
import kieranbrown.bitemp.database.QueryEquality;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import kieranbrown.bitemp.utils.QueryUtils;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.jdbc.JdbcTestUtils;
import org.springframework.transaction.PlatformTransactionManager;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import javax.transaction.Transactional;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static kieranbrown.bitemp.utils.Constants.MARIADB_END_SYSTEM_TIME;

@SpringJUnitConfig
@ActiveProfiles("evaluationTesting")
@Transactional
class SelectQueryTesting {

    //    private final List<Integer> thresholds = List.of(100, 1000, 10000, 100000, 1000000);
//        private final List<Integer> thresholds = List.of(100, 1000, 10000);
    private final List<Integer> thresholds = List.of(1000000);
    @PersistenceContext
    private EntityManager entityManager;
    private long systemTimeStart;
    @Autowired
    private DataSource dataSource;
    private long initialMemory;
    private Runtime runtime;
    private java.util.List<String> results;

    @BeforeEach
    void setup() {
        entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
        systemTimeStart = 0;
        initialMemory = 0;
        runtime = Runtime.getRuntime();
        results = new LinkedList<>();
    }

    @Transactional
    void createRows(final int entityCount, final boolean mariadb) {
        IntStream.range(1, entityCount + 1)
                .mapToObj(x -> {
                            final Trade trade = new Trade()
                                    .setBitemporalKey(new BitemporalKey.Builder()
                                            .setTradeId(UUID.randomUUID())
                                            .setValidTimeStart(ThreadLocalRandom.current().nextInt(0, 2) == 1 ? LocalDate.of(2020, 5, 2) : LocalDate.of(2020, 5, 4))
                                            .setValidTimeEnd(ThreadLocalRandom.current().nextInt(0, 4) == 3 ? LocalDate.of(2020, 5, 4) : LocalDate.of(2020, 5, 10))
                                            .build())
                                    .setStock(ThreadLocalRandom.current().nextInt(0, 4) == 3 ? "AAPL" : "MSFT")
                                    .setBuySellFlag('B')
                                    .setMarketLimitFlag('M')
                                    .setPrice(new BigDecimal("123.45"))
                                    .setVolume(250);

                            if (!mariadb) {
                                trade.setSystemTimeStart(ThreadLocalRandom.current().nextInt(0, 4) == 3 ? LocalDateTime.of(2020, 5, 3, 2, 0, 0) : LocalDateTime.of(2020, 5, 3, 3, 30, 0))
                                        .setSystemTimeEnd(ThreadLocalRandom.current().nextInt(0, 2) == 1 ? LocalDateTime.of(2020, 5, 3, 4, 0, 0, 0) : LocalDateTime.of(2020, 5, 3, 6, 0, 0));
                            }
                            return trade;
                        }
                )
                .forEach(trade -> {
                            if (mariadb)
                                entityManager.createNativeQuery("INSERT INTO reporting.trade_data (stock, price, volume, buy_sell_flag, market_limit_flag, id, valid_time_start, valid_time_end) VALUES " +
                                        "(?1,?2, ?3, ?4, ?5, ?6, ?7, ?8)")
                                        .setParameter(1,
                                                trade.getStock())
                                        .setParameter(2,
                                                trade.getPrice())
                                        .setParameter(3,
                                                trade.getVolume())
                                        .setParameter(4,
                                                trade.getBuySellFlag())
                                        .setParameter(5,
                                                trade.getMarketLimitFlag())
                                        .setParameter(6,
                                                trade.getBitemporalKey().getId())
                                        .setParameter(7,
                                                trade.getBitemporalKey().getValidTimeStart())
                                        .setParameter(8,
                                                trade.getBitemporalKey().getValidTimeEnd())
                                        .executeUpdate();
                            else {
                                entityManager.persist(trade);
                            }
                        }
                );
    }

    @Transactional
    void createRows3(final int entityCount, final boolean mariadb) {
        IntStream.range(1, entityCount + 1)
                .mapToObj(x -> {
                            final Trade trade = new Trade()
                                    .setBitemporalKey(new BitemporalKey.Builder()
                                            .setTradeId(UUID.randomUUID())
                                            .setValidTimeStart(ThreadLocalRandom.current().nextInt(0, 2) == 1 ? LocalDate.of(2020, 3, 10) : LocalDate.of(2020, 3, 13))
                                            .setValidTimeEnd(ThreadLocalRandom.current().nextInt(0, 3) == 1 ? LocalDate.of(2020, 3, 15) : LocalDate.of(2020, 3, 18))
                                            .build())
                                    .setStock(ThreadLocalRandom.current().nextInt(0, 2) == 1 ? "AAPL" : "MSFT")
                                    .setBuySellFlag('B')
                                    .setMarketLimitFlag('M')
                                    .setPrice(new BigDecimal("123.45"))
                                    .setVolume(250);

                            if (!mariadb) {
                                trade.setSystemTimeStart(ThreadLocalRandom.current().nextInt(0, 4) == 3 ? LocalDateTime.of(2020, 5, 1, 0, 0, 0) : LocalDateTime.of(2020, 5, 3, 0, 0, 0))
                                        .setSystemTimeEnd(MARIADB_END_SYSTEM_TIME);
                            }
                            return trade;
                        }
                )
                .forEach(trade -> {
                            if (mariadb)
                                entityManager.createNativeQuery("INSERT INTO reporting.trade_data (stock, price, volume, buy_sell_flag, market_limit_flag, id, valid_time_start, valid_time_end) VALUES " +
                                        "(?1,?2, ?3, ?4, ?5, ?6, ?7, ?8)")
                                        .setParameter(1,
                                                trade.getStock())
                                        .setParameter(2,
                                                trade.getPrice())
                                        .setParameter(3,
                                                trade.getVolume())
                                        .setParameter(4,
                                                trade.getBuySellFlag())
                                        .setParameter(5,
                                                trade.getMarketLimitFlag())
                                        .setParameter(6,
                                                trade.getBitemporalKey().getId())
                                        .setParameter(7,
                                                trade.getBitemporalKey().getValidTimeStart())
                                        .setParameter(8,
                                                trade.getBitemporalKey().getValidTimeEnd())
                                        .executeUpdate();
                            else {
                                entityManager.persist(trade);
                            }
                        }
                );
    }

    @Transactional
    void createBitemporalRows(final int entityCount, final boolean mariadb) {
        final Stack<String> ids = new Stack<>();
        ids.addAll(
                List.of(
                        "769fb864-f3b7-4ca5-965e-bcff80088197",
                        "769fb864-f3b7-4ca5-965e-bcff80088297",
                        "769fb864-f3b7-4ca5-965e-bcff80088397",
                        "769fb864-f3b7-4ca5-965e-bcff80088497",
                        "769fb864-f3b7-4ca5-965e-bcff80088597",
                        "769fb864-f3b7-4ca5-965e-bcff80088697",
                        "769fb864-f3b7-4ca5-965e-bcff80088797",
                        "769fb864-f3b7-4ca5-965e-bcff80088897",
                        "769fb864-f3b7-4ca5-965e-bcff80088997",
                        "769fb864-f3b7-4ca5-965e-bcff80088097"
                ).toJavaList());

        final Stream<Trade> tradeStream = IntStream.range(1, (entityCount / 10) + 1)
                .mapToObj(x -> new Trade()
                        .setBitemporalKey(new BitemporalKey.Builder()
                                .setTradeId(ids.empty() ? UUID.randomUUID() : UUID.fromString(ids.pop()))
                                .setValidTimeStart(LocalDate.of(2020, 4, 18))
                                .setValidTimeEnd(LocalDate.of(2020, 4, 19))
                                .build())
                        .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 2, 0, 0))
                        .setSystemTimeEnd(LocalDateTime.of(2020, 5, 2, 2, 30, 0))
                        .setStock("AAPL")
                        .setBuySellFlag('B')
                        .setMarketLimitFlag('M')
                        .setPrice(new BigDecimal("123.45"))
                        .setVolume(250)
                )
                .map(x -> {
                    final UUID id = x.getBitemporalKey().id;
                    final ArrayList<Trade> objects = new ArrayList<>();
                    objects.add(x);
                    objects.add(new Trade()
                            .setBitemporalKey(new BitemporalKey.Builder()
                                    .setTradeId(id)
                                    .setValidTimeStart(LocalDate.of(2020, 4, 19))
                                    .setValidTimeEnd(LocalDate.of(2020, 4, 20))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 2, 30, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 5, 2, 3, 0, 0))
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("123.45"))
                            .setVolume(250));
                    objects.add(new Trade()
                            .setBitemporalKey(new BitemporalKey.Builder()
                                    .setTradeId(id)
                                    .setValidTimeStart(LocalDate.of(2020, 4, 20))
                                    .setValidTimeEnd(LocalDate.of(2020, 4, 21))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 3, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 5, 2, 3, 30, 0))
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("123.45"))
                            .setVolume(250));
                    objects.add(new Trade()
                            .setBitemporalKey(new BitemporalKey.Builder()
                                    .setTradeId(id)
                                    .setValidTimeStart(LocalDate.of(2020, 4, 21))
                                    .setValidTimeEnd(LocalDate.of(2020, 4, 22))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 3, 30, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 5, 2, 4, 0, 0))
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("123.45"))
                            .setVolume(250));
                    objects.add(new Trade()
                            .setBitemporalKey(new BitemporalKey.Builder()
                                    .setTradeId(id)
                                    .setValidTimeStart(LocalDate.of(2020, 4, 22))
                                    .setValidTimeEnd(LocalDate.of(2020, 4, 23))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 4, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 5, 2, 4, 20, 0))
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("123.45"))
                            .setVolume(250));
                    objects.add(new Trade()
                            .setBitemporalKey(new BitemporalKey.Builder()
                                    .setTradeId(id)
                                    .setValidTimeStart(LocalDate.of(2020, 4, 23))
                                    .setValidTimeEnd(LocalDate.of(2020, 4, 24))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 4, 20, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 5, 2, 4, 40, 0))
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("123.45"))
                            .setVolume(250));
                    objects.add(new Trade()
                            .setBitemporalKey(new BitemporalKey.Builder()
                                    .setTradeId(id)
                                    .setValidTimeStart(LocalDate.of(2020, 4, 24))
                                    .setValidTimeEnd(LocalDate.of(2020, 4, 25))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 4, 40, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 5, 2, 5, 0, 0))
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("123.45"))
                            .setVolume(250));
                    objects.add(new Trade()
                            .setBitemporalKey(new BitemporalKey.Builder()
                                    .setTradeId(id)
                                    .setValidTimeStart(LocalDate.of(2020, 4, 25))
                                    .setValidTimeEnd(LocalDate.of(2020, 4, 26))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 5, 0, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 5, 2, 5, 10, 0))
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("123.45"))
                            .setVolume(250));
                    objects.add(new Trade()
                            .setBitemporalKey(new BitemporalKey.Builder()
                                    .setTradeId(id)
                                    .setValidTimeStart(LocalDate.of(2020, 4, 26))
                                    .setValidTimeEnd(LocalDate.of(2020, 4, 27))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 5, 10, 0))
                            .setSystemTimeEnd(LocalDateTime.of(2020, 5, 2, 5, 30, 0))
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("123.45"))
                            .setVolume(250));
                    objects.add(new Trade()
                            .setBitemporalKey(new BitemporalKey.Builder()
                                    .setTradeId(id)
                                    .setValidTimeStart(LocalDate.of(2020, 4, 27))
                                    .setValidTimeEnd(LocalDate.of(2020, 4, 28))
                                    .build())
                            .setSystemTimeStart(LocalDateTime.of(2020, 5, 2, 5, 30, 0))
                            .setSystemTimeEnd(MARIADB_END_SYSTEM_TIME)
                            .setStock("AAPL")
                            .setBuySellFlag('B')
                            .setMarketLimitFlag('M')
                            .setPrice(new BigDecimal("123.45"))
                            .setVolume(250));
                    return objects;
                })
                .flatMap(Collection::stream);
        tradeStream.forEach(trade -> {
//            if (mariadb) {
//                entityManager.createNativeQuery(
//                        "INSERT INTO reporting.trade_data (stock, price, volume, buy_sell_flag, market_limit_flag, id, valid_time_start, valid_time_end, system_time_start, system_time_end) VALUES " +
//                                "(?1,?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)")
//                        .setParameter(1,
//                                trade.getStock())
//                        .setParameter(2,
//                                trade.getPrice())
//                        .setParameter(3,
//                                trade.getVolume())
//                        .setParameter(4,
//                                trade.getBuySellFlag())
//                        .setParameter(5,
//                                trade.getMarketLimitFlag())
//                        .setParameter(6,
//                                trade.getBitemporalKey().getId())
//                        .setParameter(7,
//                                trade.getBitemporalKey().getValidTimeStart())
//                        .setParameter(8,
//                                trade.getBitemporalKey().getValidTimeEnd())
//                        .setParameter(9, trade.getSystemTimeStart())
//                        .setParameter(10, trade.getSystemTimeEnd())
//                        .executeUpdate();
            entityManager.merge(trade);
//            } else {
//                entityManager.persist(trade);
//            }
        });
    }

    @Test
    @DisplayName("total revenue of sales for stock A for valid time now, as of system time yesterday")
    void implementationSelectQueryOne() throws InterruptedException {
        testQuery(() -> QueryBuilderFactory.select(Trade.class)
                .where("stock", QueryEquality.EQUALS, "AAPL")
                .where("valid_time_start", QueryEquality.GREATER_THAN_EQUAL_TO, LocalDate.of(2020, 3, 12))
                .systemTimeAsOf(LocalDateTime.of(2020, 4, 28, 0, 0, 0))
                .execute(entityManager)
                .getResults()
                .toJavaList(), false);
    }

    @Test
    @DisplayName("all records with system time as of X, with valid time preceding X")
    void implementationSelectQueryThree() throws InterruptedException {
        testQuery(() -> QueryBuilderFactory.select(Trade.class)
                .systemTimeAsOf(LocalDateTime.of(2020, 5, 2, 3, 0, 0))
                .validTimePrecedes(LocalDate.of(2020, 3, 16))
                .execute(entityManager)
                .getResults()
                .toJavaList(), false);
    }

    @Test
    @DisplayName("all records with system time between X and Y, with valid time overlapping A and B")
    void implementationSelectQueryFour() throws InterruptedException {
        testQuery(() -> QueryBuilderFactory.select(Trade.class)
                .systemTimeBetween(LocalDateTime.of(2020, 5, 3, 3, 0, 0, 0), LocalDateTime.of(2020, 5, 3, 5, 0, 0, 0))
                .validTimeOverlaps(LocalDate.of(2020, 5, 3), LocalDate.of(2020, 5, 5))
                .execute(entityManager)
                .getResults()
                .toJavaList(), false);
    }

    @Test
    @DisplayName("total revenue of sales for stock A for valid time now, as of system time yesterday")
    void implementedSelectQueryOne() throws InterruptedException {
        testQuery(() -> (java.util.List<Trade>) entityManager.createNativeQuery(
                "select * from reporting.trade_data" +
                        " for SYSTEM_TIME AS OF TIMESTAMP '2020-05-04 00:00:00.000000'" +
                        " where stock = 'AAPL'" +
                        " and valid_time_start >= TIMESTAMP '2020-03-12 00:00:00.000000'", Trade.class)
                .getResultList(), true);
    }

    @Test
    @DisplayName("all records with system time as of X, with valid time preceding X")
    void implementedSelectQueryThree() throws InterruptedException {
        testQueryMariadb3();
    }

    private void testQueryMariadb3() throws
            InterruptedException {
        for (int x : thresholds) {
            createRows3(x, true);
            final LocalDateTime time = getTime();
            System.gc();
            System.out.println("starting run of size " + x);
            Thread.sleep(5000);
            systemTimeStart = System.currentTimeMillis();
            initialMemory = runtime.totalMemory() - runtime.freeMemory();
            final java.util.List<Trade> trades = (java.util.List<Trade>) entityManager.createNativeQuery(
                    "select * from reporting.trade_data" +
                            " for SYSTEM_TIME AS OF TIMESTAMP " +
                            QueryUtils.toString(time) + "" +
                            " where valid_time_end <= '2020-03-16'", Trade.class)
                    .getResultList();
            System.out.println("entries in table:" + JdbcTestUtils.countRowsInTable(new JdbcTemplate(dataSource), "reporting.trade_data"));
            System.out.println("number of results:" + trades.size());
            results.add(String.format("%,17d | %,25d | %,23d", x, System.currentTimeMillis() - systemTimeStart, (runtime.totalMemory() - runtime.freeMemory()) - initialMemory));
            entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
        }

        System.out.println("Number of Objects | Runtime (in milliseconds) | Memory Usage (in bytes)");
        results.forEach(System.out::println);
    }

    private LocalDateTime getTime() {
        final java.util.List<Trade> resultList = entityManager.createNativeQuery("select * from reporting.trade_data", Trade.class)
                .getResultList();
        return resultList.get(0).getSystemTimeStart();
    }

    @Test
    @DisplayName("all records with system time between X and Y, with valid time overlapping A and B")
    void implementedSelectQueryFour() throws InterruptedException {
        testQuery(() -> QueryBuilderFactory.select(Trade.class)
                .systemTimeBetween(LocalDateTime.of(2020, 5, 3, 3, 0, 0, 0), LocalDateTime.of(2020, 5, 3, 5, 0, 0, 0))
                .validTimeOverlaps(LocalDate.of(2020, 5, 3), LocalDate.of(2020, 5, 5))
                .execute(entityManager)
                .getResults()
                .toJavaList(), false);
        testQueryMariadb4();
    }

    private void testQueryMariadb4() throws
            InterruptedException {
        for (int x : thresholds) {
            createRows3(x, true);
            final LocalDateTime time = getTime4();
            System.gc();
            System.out.println("starting run of size " + x);
            Thread.sleep(5000);
            systemTimeStart = System.currentTimeMillis();
            initialMemory = runtime.totalMemory() - runtime.freeMemory();
            final java.util.List<Trade> trades = (java.util.List<Trade>) entityManager.createNativeQuery(
                    "select * from reporting.trade_data" +
                            " for SYSTEM_TIME BETWEEN " +
                            QueryUtils.toString(time) +
                            " AND " +
                            QueryUtils.toString(time) +
                            " where valid_time_end <= '2020-03-16'", Trade.class)
                    .getResultList();
            System.out.println("entries in table:" + JdbcTestUtils.countRowsInTable(new JdbcTemplate(dataSource), "reporting.trade_data"));
            System.out.println("number of results:" + trades.size());
            results.add(String.format("%,17d | %,25d | %,23d", x, System.currentTimeMillis() - systemTimeStart, (runtime.totalMemory() - runtime.freeMemory()) - initialMemory));
            entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
        }

        System.out.println("Number of Objects | Runtime (in milliseconds) | Memory Usage (in bytes)");
        results.forEach(System.out::println);
    }

    private LocalDateTime getTime4() {
        final java.util.List<Trade> resultList = entityManager.createNativeQuery("select * from reporting.trade_data", Trade.class)
                .getResultList();
        return resultList.get(0).getSystemTimeStart();
    }

    private void testQuery(final Supplier<java.util.List<Trade>> query, final boolean mariadb) throws
            InterruptedException {
        for (int x : thresholds) {
            createRows3(x, mariadb);
            System.gc();
            System.out.println("starting run of size " + x);
            Thread.sleep(5000);
            systemTimeStart = System.currentTimeMillis();
            initialMemory = runtime.totalMemory() - runtime.freeMemory();
            final java.util.List<Trade> trades = query.get();
            System.out.println("entries in table:" + JdbcTestUtils.countRowsInTable(new JdbcTemplate(dataSource), "reporting.trade_data"));
            System.out.println("number of results:" + trades.size());
            results.add(String.format("%,17d | %,25d | %,23d", x, System.currentTimeMillis() - systemTimeStart, (runtime.totalMemory() - runtime.freeMemory()) - initialMemory));
            entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
        }

        System.out.println("Number of Objects | Runtime (in milliseconds) | Memory Usage (in bytes)");
        results.forEach(System.out::println);
    }

    private void testQueryBitemporal(final Supplier<java.util.List<Trade>> query, final boolean mariadb) throws
            InterruptedException {
        for (int x : thresholds) {
            createBitemporalRows(x, mariadb);
            System.gc();
            System.out.println("starting run of size " + x);
            Thread.sleep(5000);
            systemTimeStart = System.currentTimeMillis();
            initialMemory = runtime.totalMemory() - runtime.freeMemory();
            final java.util.List<Trade> trades = query.get();
            System.out.println("entries in table:" + JdbcTestUtils.countRowsInTable(new JdbcTemplate(dataSource), "reporting.trade_data"));
            System.out.println("number of results:" + trades.size());
            results.add(String.format("%,17d | %,25d | %,23d", x, System.currentTimeMillis() - systemTimeStart, (runtime.totalMemory() - runtime.freeMemory()) - initialMemory));
            entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
        }

        System.out.println("Number of Objects | Runtime (in milliseconds) | Memory Usage (in bytes)");
        results.forEach(System.out::println);
    }

    @Configuration
    @Profile("evaluationTesting")
    static class DataSourceConfig {

        @Bean(initMethod = "migrate")
        public Flyway flyway(final DataSource dataSource) {
            return Flyway.configure().dataSource(dataSource).schemas("reporting").load();
        }

        @Bean
        public DataSource dataSource() {
            System.out.println("***************************");
            System.out.println("DATA SOURCE CREATION HERE");
            System.out.println("***************************");
            return DataSourceBuilder.create().driverClassName("com.mysql.cj.jdbc.Driver").url("jdbc:mysql://localhost:3306").username("root").password("password").build();
        }

        @Bean
        public LocalContainerEntityManagerFactoryBean entityManagerFactory() {
            final LocalContainerEntityManagerFactoryBean em = new LocalContainerEntityManagerFactoryBean();
            em.setDataSource(dataSource());
            em.setPackagesToScan("kieranbrown.bitemp");

            final JpaVendorAdapter vendorAdapter = new HibernateJpaVendorAdapter();
            em.setJpaVendorAdapter(vendorAdapter);
            em.setJpaProperties(additionalProperties());

            return em;
        }

        @Bean
        public PlatformTransactionManager transactionManager(EntityManagerFactory emf) {
            JpaTransactionManager transactionManager = new JpaTransactionManager();
            transactionManager.setEntityManagerFactory(emf);

            return transactionManager;
        }

        @Bean
        public PersistenceExceptionTranslationPostProcessor exceptionTranslation() {
            return new PersistenceExceptionTranslationPostProcessor();
        }

        Properties additionalProperties() {
            Properties properties = new Properties();
            properties.setProperty("hibernate.hbm2ddl.auto", "create-drop");
            properties.setProperty("hibernate.dialect", "org.hibernate.dialect.MySQL5Dialect");

            return properties;
        }
    }

}

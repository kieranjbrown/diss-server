package kieranbrown.bitemp.evaluation;

import com.opencsv.CSVReader;
import com.opencsv.bean.ColumnPositionMappingStrategy;
import com.opencsv.bean.CsvToBean;
import com.opencsv.bean.CsvToBeanBuilder;
import io.vavr.collection.List;
import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;
import kieranbrown.bitemp.database.InsertQueryBuilder;
import kieranbrown.bitemp.database.InvalidPeriodException;
import kieranbrown.bitemp.database.QueryBuilderFactory;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.annotation.PersistenceExceptionTranslationPostProcessor;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.JpaVendorAdapter;
import org.springframework.orm.jpa.LocalContainerEntityManagerFactoryBean;
import org.springframework.orm.jpa.vendor.HibernateJpaVendorAdapter;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.transaction.PlatformTransactionManager;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceContext;
import javax.sql.DataSource;
import javax.transaction.Transactional;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

@Disabled
@SpringJUnitConfig
@ActiveProfiles("evaluationTesting")
@Transactional
class InsertQueryTesting {

    //    private final List<Integer> thresholds = List.of(100, 1000, 10000, 100000, 1000000);
    private final List<Integer> thresholds = List.of(1);
    @PersistenceContext
    private EntityManager entityManager;
    @Autowired
    private DataSource dataSource;
    private long systemTimeStart;
    private long initialMemory;
    private int objectCount;
    private Runtime runtime;
    private InsertQueryBuilder<Trade> queryBuilder;
    private java.util.List<String> results;

    @BeforeEach
    void setup() {
        entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
        systemTimeStart = 0;
        objectCount = 0;
        initialMemory = 0;
        runtime = Runtime.getRuntime();
        results = new LinkedList<>();
    }

    private java.util.stream.Stream<Trade> readObjects() throws IOException {
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
                .withFilter(line -> !line[2].equals("123.4"))
                .build();
        return csvToBean.stream();
    }

    @Test
    void memorySize() {
        long objectSize = ObjectSizeCalculator.getObjectSize(new Trade()
                .setVolume(123)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("AAPL")
                .setSystemTimeStart(LocalDateTime.of(2020, 2, 1, 2, 2, 2))
                .setSystemTimeEnd(LocalDateTime.of(2020, 2, 2, 2, 2, 2))
                .setBitemporalKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setValidTimeStart(LocalDate.of(2020, 2, 2)).setValidTimeEnd(LocalDate.of(2020, 2, 3)).build()));
        System.out.println("mine:" + objectSize);

        objectSize = ObjectSizeCalculator.getObjectSize(new Trade()
                .setVolume(123)
                .setPrice(new BigDecimal("123.45"))
                .setMarketLimitFlag('M')
                .setBuySellFlag('B')
                .setStock("AAPL")
                .setBitemporalKey(new BitemporalKey.Builder().setTradeId(UUID.randomUUID()).setValidTimeStart(LocalDate.of(2020, 2, 2)).setValidTimeEnd(LocalDate.of(2020, 2, 3)).build()));
        System.out.println("theirs:" + objectSize);

        System.out.println("size of my query: " + ObjectSizeCalculator.getObjectSize("INSERT INTO reporting.trade_data (stock, price, volume, buy_sell_flag, market_limit_flag, id, valid_time_start, valid_time_end, system_time_start, system_time_end) VALUES ('AAPL', 123.45, 250, 'B', 'M', '1c876b60-7cf3-484e-b4d8-aa510838dce0', '2020-01-20', '2020-01-21', '2020-02-02 00:00:00.000000', '2038-01-19 03:14:07.000000')"));
        System.out.println("size of their query: " + ObjectSizeCalculator.getObjectSize("INSERT INTO reporting.trade_data (stock, price, volume, buy_sell_flag, market_limit_flag, id, valid_time_start, valid_time_end) VALUES ('AAPL', 123.45, 250, 'B', 'M', '1c876b60-7cf3-484e-b4d8-aa510838dce0', '2020-01-20', '2020-01-21')"));
    }

    @Test
    void implementationInsertQuery() throws IOException {
        insertQuery(this::persistImplementation);
    }

    @Test
    void mariadbInsertQuery() throws IOException {
        insertQuery(this::persistMariadb);
    }

    private void insertQuery(final Consumer<Trade> persist) throws IOException {
        final java.util.stream.Stream<Trade> trades = readObjects();
        systemTimeStart = System.currentTimeMillis();
        System.gc();
        initialMemory = runtime.totalMemory() - runtime.freeMemory();
        this.queryBuilder = QueryBuilderFactory.insert(Trade.class);
        trades.forEach(persist);
        System.out.println("Number of Objects | Runtime (in milliseconds) | Memory Usage (in bytes)");
        results.forEach(System.out::println);
    }

    private void persistMariadb(final Trade trade) {
        if (objectCount >= thresholds.last()) {
            return;
        }
        entityManager.createNativeQuery(
                "INSERT INTO reporting.trade_data (stock, price, volume, buy_sell_flag, market_limit_flag, id, valid_time_start, valid_time_end) VALUES " +
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
        System.out.println("objectCount:" + objectCount);
        if (thresholds.contains(++objectCount)) {
            results.add(String.format("%,17d | %,25d | %,23d", objectCount, System.currentTimeMillis() - systemTimeStart, (runtime.totalMemory() - runtime.freeMemory()) - initialMemory));
        }
    }

    private void persistImplementation(final Trade trade) {
        if (objectCount >= thresholds.last()) {
            return;
        }
        try {
            queryBuilder.from(trade).execute(entityManager);
        } catch (final InvalidPeriodException e) {
            throw new RuntimeException("error", e);
        }
        System.out.println("objectCount:" + objectCount);
        if (thresholds.contains(++objectCount)) {
            results.add(String.format("%,17d | %,25d | %,23d", objectCount, System.currentTimeMillis() - systemTimeStart, (runtime.totalMemory() - runtime.freeMemory()) - initialMemory));
        }
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
            System.out.println("creating right datasource");
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

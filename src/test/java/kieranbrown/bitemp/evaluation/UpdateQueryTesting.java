package kieranbrown.bitemp.evaluation;

import io.vavr.Tuple3;
import io.vavr.collection.List;
import kieranbrown.bitemp.database.InvalidPeriodException;
import kieranbrown.bitemp.database.QueryBuilderFactory;
import kieranbrown.bitemp.database.SingleQueryFilter;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import kieranbrown.bitemp.utils.QueryUtils;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
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
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static kieranbrown.bitemp.database.QueryEquality.*;
import static kieranbrown.bitemp.utils.Constants.MARIADB_END_SYSTEM_TIME;

@SpringJUnitConfig
@ActiveProfiles("evaluationTesting")
@Transactional
class UpdateQueryTesting {

    //        private final List<Integer> thresholds = List.of(100, 1000, 10000, 100000);
//        private final List<Integer> thresholds = List.of(100, 1000);
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
    void createRows3(final int entityCount, final boolean mariadb) {
        IntStream.range(1, entityCount + 1)
                .mapToObj(x -> {
                            final Trade trade = new Trade()
                                    .setBitemporalKey(new BitemporalKey.Builder()
                                            .setTradeId(UUID.randomUUID())
                                            .setValidTimeStart(ThreadLocalRandom.current().nextInt(0, 2) == 1 ? LocalDate.of(2020, 3, 10) : LocalDate.of(2020, 3, 13))
                                            .setValidTimeEnd(ThreadLocalRandom.current().nextInt(0, 7) == 6 ? LocalDate.of(2020, 3, 15) : LocalDate.of(2020, 3, 18))
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

    @Test
    void myTest() throws InterruptedException, InvalidPeriodException {
        testQuery();
    }

    private void testQuery() throws
            InterruptedException, InvalidPeriodException {
        for (int x : thresholds) {
            createRows3(x, false);
            final LocalDateTime time = getTime();
            System.gc();
            System.out.println("starting run of size " + x);
            Thread.sleep(5000);
            systemTimeStart = System.currentTimeMillis();
            initialMemory = runtime.totalMemory() - runtime.freeMemory();
            QueryBuilderFactory.update(Trade.class)
                    .where(new SingleQueryFilter(new Tuple3<>("system_time_start", LESS_THAN_EQUAL_TO, time)))
                    .where(new SingleQueryFilter(new Tuple3<>("system_time_end", GREATER_THAN, time)))
                    .where(new SingleQueryFilter("stock", EQUALS, "AAPL"))
                    .set("buy_sell_flag", "s")
                    .execute(entityManager);
            System.out.println("entries in table:" + JdbcTestUtils.countRowsInTable(new JdbcTemplate(dataSource), "reporting.trade_data"));
            results.add(String.format("%,17d | %,25d | %,23d", x, System.currentTimeMillis() - systemTimeStart, (runtime.totalMemory() - runtime.freeMemory()) - initialMemory));
            entityManager.createNativeQuery("delete from reporting.trade_data").executeUpdate();
        }

        System.out.println("Number of Objects | Runtime (in milliseconds) | Memory Usage (in bytes)");
        results.forEach(System.out::println);
    }

    @Test
    void mariadbTest() throws InterruptedException {
        testQueryMariadb();
    }

    private void testQueryMariadb() throws
            InterruptedException {
        for (int x : thresholds) {
            createRows3(x, true);
            final LocalDateTime time = getTime();
            System.gc();
            System.out.println("starting run of size " + x);
            Thread.sleep(5000);
            systemTimeStart = System.currentTimeMillis();
            initialMemory = runtime.totalMemory() - runtime.freeMemory();
            final int updates = entityManager
                    .createNativeQuery("update reporting.trade_data for system_time as of timestamp " +
                            QueryUtils.toString(time) +
                            " set buy_sell_flag = 's'"
                            +
                            " where stock = 'AAPL'"
                    )
                    .executeUpdate();
            System.out.println("rows update:" + updates);
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

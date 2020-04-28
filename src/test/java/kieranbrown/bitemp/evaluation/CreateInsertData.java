package kieranbrown.bitemp.evaluation;

import com.opencsv.CSVWriter;
import com.opencsv.bean.StatefulBeanToCsv;
import com.opencsv.bean.StatefulBeanToCsvBuilder;
import com.opencsv.exceptions.CsvDataTypeMismatchException;
import com.opencsv.exceptions.CsvRequiredFieldEmptyException;
import io.vavr.collection.Stream;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import kieranbrown.bitemp.utils.Constants;

import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class CreateInsertData {

    public static void main(final String[] args) throws IOException, CsvDataTypeMismatchException, CsvRequiredFieldEmptyException {
        final StatefulBeanToCsvBuilder statefulBeanToCsvBuilder = new StatefulBeanToCsvBuilder(new FileWriter("InsertData.csv"));
        final StatefulBeanToCsv statefulBeanToCsv = statefulBeanToCsvBuilder
                .withQuotechar(CSVWriter.NO_ESCAPE_CHARACTER)

                .build();

        final List<UUID> repeatedIds = new ArrayList<>();

        statefulBeanToCsv.write(
                Stream.range(1, 1000025)
                        .map(x -> {
//                                    if (x <= 200005) {
//                                        final UUID id = UUID.randomUUID();
//                                        repeatedIds.add(id);
//
//                                        return new Trade()
//                                                .setBitemporalKey(new BitemporalKey.Builder()
//                                                        .setTradeId(id)
//                                                        .setValidTimeStart(LocalDate.of(2020, 1, 20))
//                                                        .setValidTimeEnd(LocalDate.of(2020, 1, 21))
//                                                        .build())
//                                                .setSystemTimeStart(LocalDateTime.now())
//                                                .setSystemTimeEnd(MARIADB_END_SYSTEM_TIME)
//                                                .setStock("AAPL")
//                                                .setBuySellFlag('B')
//                                                .setMarketLimitFlag('M')
//                                                .setPrice(new BigDecimal("123.45"))
//                                                .setVolume(250);
//
//                                    }
//                                    if (x % 4 == 0) {
//                                        final UUID id = repeatedIds.get(0);
//                                        repeatedIds.remove(0);
//                                        return new Trade()
//                                                .setBitemporalKey(new BitemporalKey.Builder()
//                                                        .setTradeId(id)
//                                                        .setValidTimeStart(LocalDate.of(2020, 1, 23))
//                                                        .setValidTimeEnd(LocalDate.of(2020, 1, 25))
//                                                        .build())
//                                                .setSystemTimeStart(LocalDateTime.now())
//                                                .setSystemTimeEnd(MARIADB_END_SYSTEM_TIME)
//                                                .setStock("AAPL")
//                                                .setBuySellFlag('B')
//                                                .setMarketLimitFlag('M')
//                                                .setPrice(new BigDecimal("123.45"))
//                                                .setVolume(250);
//
//                                    }

                                    return new Trade()
                                            .setBitemporalKey(new BitemporalKey.Builder()
                                                    .setTradeId(UUID.randomUUID())
                                                    .setValidTimeStart(LocalDate.of(2020, 1, 20))
                                                    .setValidTimeEnd(LocalDate.of(2020, 1, 21))
                                                    .build())
                                            .setSystemTimeStart(LocalDateTime.now())
                                            .setSystemTimeEnd(Constants.MARIADB_END_SYSTEM_TIME)
                                            .setStock("AAPL")
                                            .setBuySellFlag('B')
                                            .setMarketLimitFlag('M')
                                            .setPrice(new BigDecimal("123.45"))
                                            .setVolume(250);
                                }
                        )
                        .toJavaList()

        );
    }
}

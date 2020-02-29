package kieranbrown.bitemp.database;

import kieranbrown.bitemp.utils.QueryUtils;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

class QueryUtilsTest {
    @Test
    void toStringThrowsIfInputIsNull() {
        assertThat(assertThrows(NullPointerException.class, () -> QueryUtils.toString(null)))
                .hasMessage("input cannot be null");
    }

    @Test
    void toStringFormatsDatesCorrectly() {
        assertThat(QueryUtils.toString(LocalDate.of(2020, 2, 20))).isNotNull().isEqualTo("'2020-02-20'");
        assertThat(QueryUtils.toString(LocalDate.of(2020, 1, 20))).isNotNull().isEqualTo("'2020-01-20'");
        assertThat(QueryUtils.toString(LocalDate.of(2020, 10, 20))).isNotNull().isEqualTo("'2020-10-20'");

        assertThat(QueryUtils.toString(new Date(2020, 1, 20, 13, 43, 0))).isNotNull().isEqualTo("'2020-01-20 13:43:00.000000'");
        assertThat(QueryUtils.toString(new Date(2020, 1, 20, 0, 0, 0))).isNotNull().isEqualTo("'2020-01-20 00:00:00.000000'");
    }

    @Test
    void toStringFormatsStringsCorrectly() {
        assertThat(QueryUtils.toString("AMZN")).isNotNull().isEqualTo("'AMZN'");
    }

    @Test
    void toStringFormatsUUIDCorrectly() {
        final UUID uuid = UUID.randomUUID();
        assertThat(QueryUtils.toString(uuid)).isNotNull().isEqualTo("'" + uuid + "'");
    }

    @Test
    void toStringFormatsBigDecimalCorrectly() {
        assertThat(QueryUtils.toString(new BigDecimal("123.45"))).isNotNull().isEqualTo("123.45");
    }
}
package kieranbrown.bitemp.utils;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.util.Date;
import java.util.UUID;

public final class QueryUtils {
    private QueryUtils() {
    }

    //TODO: move tests from QueryTest to separate test class for this
    public static String toString(final Object o) {
        if (o.getClass().equals(Date.class)) {
            final Date date = (Date) o;
            return String.format("'%s-%s-%s %s:%s:%s.000000'",
                    date.getYear(),
                    StringUtils.leftPad(String.valueOf(date.getMonth()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getDate()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getHours()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getMinutes()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getSeconds()), 2, "0"));
        } else if (o.getClass().equals(LocalDate.class)) {
            final LocalDate date = (LocalDate) o;
            return String.format("'%s-%s-%s'",
                    date.getYear(),
                    StringUtils.leftPad(String.valueOf(date.getMonthValue()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getDayOfMonth()), 2, "0"));
        } else if (o.getClass().equals(String.class) || o.getClass().equals(Character.class) || o.getClass().equals(UUID.class)) {
            return String.format("'%s'", o);
        }
        return o.toString();
    }
}

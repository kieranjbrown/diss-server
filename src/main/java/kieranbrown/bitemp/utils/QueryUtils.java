package kieranbrown.bitemp.utils;

import org.apache.commons.lang3.StringUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.UUID;

public final class QueryUtils {
    private QueryUtils() {
    }

    public static String toString(final Object o) {
//        if (o == null) return "'null'";
        if (o.getClass().equals(LocalDateTime.class)) {
            final LocalDateTime date = (LocalDateTime) o;
            return String.format("'%s-%s-%s %s:%s:%s.%s'",
                    date.getYear(),
                    StringUtils.leftPad(String.valueOf(date.getMonthValue()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getDayOfMonth()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getHour()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getMinute()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getSecond()), 2, "0"),
                    StringUtils.substring(String.valueOf(date.getNano()), 0, 6));
        } else if (o.getClass().equals(LocalDate.class)) {
            final LocalDate date = (LocalDate) o;
            return String.format("'%s-%s-%s'",
                    date.getYear(),
                    StringUtils.leftPad(String.valueOf(date.getMonthValue()), 2, "0"),
                    StringUtils.leftPad(String.valueOf(date.getDayOfMonth()), 2, "0"));
        } else if (o.getClass().equals(String.class) || o.getClass().equals(Character.class) || o.getClass().equals(UUID.class)) {
            if (o.getClass().equals(String.class) && "CURRENT_TIMESTAMP".equals(o)) {
                return (String) o;
            }
            return String.format("'%s'", o);
        }
        return o.toString();
    }
}

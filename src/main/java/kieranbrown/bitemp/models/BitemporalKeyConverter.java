package kieranbrown.bitemp.models;

import com.opencsv.bean.AbstractBeanField;
import com.opencsv.exceptions.CsvConstraintViolationException;
import com.opencsv.exceptions.CsvDataTypeMismatchException;

import java.time.LocalDate;
import java.util.UUID;

public class BitemporalKeyConverter extends AbstractBeanField {
    @Override
    protected Object convert(String s) throws CsvDataTypeMismatchException, CsvConstraintViolationException {
        System.out.println("convert:" + s);
        final String[] key = s.split(",");
        return new BitemporalKey().setId(UUID.fromString(key[0])).setValidTimeStart(LocalDate.parse(key[1])).setValidTimeEnd(LocalDate.parse(key[2]));
    }
}

package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.BitemporalModel;
import kieranbrown.bitemp.utils.Constants;

import javax.persistence.Column;
import javax.persistence.EntityManager;
import java.lang.reflect.Field;
import java.util.Arrays;

public class InsertQueryBuilder<T extends BitemporalModel<T>> {
    private final Class<T> queryClass;
    private InsertQuery<T> query;
    private Stream<T> objects;

    InsertQueryBuilder(final Class<T> queryClass) {
        this.queryClass = queryClass;
        this.query = new InsertQuery<>(queryClass);
        objects = Stream.of();
    }

    public InsertQueryBuilder<T> from(final T object) {
        objects = objects.append(object);
        return this;
    }

    @SafeVarargs
    public final InsertQueryBuilder<T> fromAll(final T... objects) {
        this.objects = this.objects.appendAll(Arrays.asList(objects));
        return this;
    }

    public InsertQueryBuilder<T> fromAll(final List<T> objects) {
        this.objects = this.objects.appendAll(objects);
        return this;
    }

    public InsertQueryBuilder<T> fromAll(final Stream<T> objects) {
        this.objects = this.objects.appendAll(objects);
        return this;
    }

    public InsertQueryBuilder<T> execute(final EntityManager entityManager) throws InvalidPeriodException {
        for (T x : objects) {
            final BitemporalKey key = x.getBitemporalKey();
            if (key.validTimeEnd.isBefore(key.validTimeStart)) {
                throw new InvalidPeriodException(String.format("Valid Time End is before Start for ID = '%s'", key.getId()));
            }
        }
        query.addFields(getFields());
        entityManager.createNativeQuery(query.build()).executeUpdate();
        reset();
        return this;
    }

    private void reset() {
        objects = Stream.of();
        query = new InsertQuery<>(queryClass);
    }

    private List<List<Tuple2<String, Object>>> getFields() {
        return objects.map(o -> List.of(queryClass.getDeclaredFields())
                .map(x -> new Tuple2<>(x, getFieldName(x)))
                .map(x -> new Tuple2<>(getColumnName(x._1), getFieldValue(x._2, o)))
                .appendAll(
                        List.of(new Tuple2<>("id", o.getBitemporalKey().getId()),
                                new Tuple2<>("valid_time_start", o.getBitemporalKey().getValidTimeStart()),
                                new Tuple2<>("valid_time_end", o.getBitemporalKey().getValidTimeEnd()),
                                new Tuple2<>("system_time_start", "CURRENT_TIMESTAMP"),
                                new Tuple2<>("system_time_end", Constants.MARIADB_END_SYSTEM_TIME))))
                .toList();
    }

    private String getFieldName(final Field field) {
        return field.getName();
    }

    private Object getFieldValue(final String fieldName, final T object) {
        try {
            final Field declaredField = queryClass.getDeclaredField(fieldName);
            declaredField.setAccessible(true);
            return declaredField.get(object);
        } catch (final NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(String.format("error retrieving value for field %s", fieldName), e);
        }
    }

    private String getColumnName(final Field field) {
        final String annotationName = field.getAnnotation(Column.class).name();
        return "".equals(annotationName) ? field.getName() : annotationName;
    }
}

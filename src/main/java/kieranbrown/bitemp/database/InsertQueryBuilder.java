package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.BitemporalModel;
import kieranbrown.bitemp.models.Trade;

import javax.persistence.Column;
import javax.persistence.EntityManager;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.Arrays;

public class InsertQueryBuilder<T extends BitemporalModel<T>> {
    private final Class<T> queryClass;
    private final InsertQuery<T> query;
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

    public InsertQueryBuilder<T> execute(final EntityManager entityManager) throws OverlappingKeyException {
        //TODO: remove coupling? InsertQueryBuilder uses UpdateQueryBuilder and vice versa
        QueryBuilderFactory.update(Trade.class)
                .set("system_time_end", LocalDateTime.now())
                .where(new OrQueryFilter(objects.map(o -> o.getBitemporalKey().getId()).map(o -> new SingleQueryFilter("id", QueryEquality.EQUALS, o))))
                .where(new SingleQueryFilter("system_time_end", QueryEquality.EQUALS, LocalDateTime.of(9999, 12, 31, 0, 0, 0)))
                .execute(entityManager);

        final SelectQueryBuilder<T> selectQueryBuilder = QueryBuilderFactory.select(queryClass);
        objects.map(BitemporalModel::getBitemporalKey)
                .forEach(x -> selectQueryBuilder.where(SelectQueryBuilder.validTimeOverlaps.apply(x.getValidTimeStart(), x.getValidTimeEnd())));

        final List<T> overlappingKeys = selectQueryBuilder.execute(entityManager).getResults();
        if (overlappingKeys.length() > 0) {
            throw new OverlappingKeyException(
                    String.format("overlapping valid time for ids = %s",
                            overlappingKeys.map(BitemporalModel::getBitemporalKey)
                                    .map(BitemporalKey::getId)
                                    .map(x -> String.format("'%s'", x))
                                    .mkString(", ")));
        }

        query.addFields(getFields());
        entityManager.createNativeQuery(query.build()).executeUpdate();
        return this;
    }

    private List<List<Tuple2<String, Object>>> getFields() {
        return objects.map(o -> List.of(queryClass.getDeclaredFields())
                .map(x -> new Tuple2<>(x, getFieldName(x)))
                .map(x -> new Tuple2<>(getColumnName(x._1), getFieldValue(x._2, o)))
                .appendAll(
                        List.of(new Tuple2<>("id", o.getBitemporalKey().getId()),
                                new Tuple2<>("valid_time_start", o.getBitemporalKey().getValidTimeStart()),
                                new Tuple2<>("valid_time_end", o.getBitemporalKey().getValidTimeEnd()),
                                new Tuple2<>("system_time_start", o.getSystemTimeStart()),
                                new Tuple2<>("system_time_end", o.getSystemTimeEnd()))))
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

package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import kieranbrown.bitemp.models.BitemporalModel;

import javax.persistence.Column;
import javax.persistence.EntityManager;
import java.lang.reflect.Field;
import java.time.LocalDateTime;
import java.util.Arrays;

public class InsertQueryBuilder<T extends BitemporalModel<T>> {
    private final Class<T> queryClass;
    private final InsertQuery<T> query;
    private SelectQuery<T> selectQuery;
    private Stream<T> objects;

    InsertQueryBuilder(final Class<T> queryClass) {
        this.queryClass = queryClass;
        this.query = new InsertQuery<>(queryClass);
        this.selectQuery = new SelectQuery<>(QueryType.SELECT, queryClass);
        objects = Stream.of();
    }

    public InsertQueryBuilder<T> from(final T object) {
        objects = objects.append(object);
        return this;
    }

    public InsertQueryBuilder<T> fromAll(final T... objects) {
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

    public InsertQueryBuilder<T> execute(final DataSource dataSource) {
        final List<List<Tuple2<String, Object>>> map = objects.map(o -> {
            return List.of(queryClass.getDeclaredFields())
                    .map(x -> new Tuple2<>(x, getFieldName(x)))
                    .map(x -> new Tuple2<>(getColumnName(x._1), getFieldValue(x._2, o)))
                    .appendAll(
                            List.of(new Tuple2<>("id", o.getTradeKey().getId()),
                                    new Tuple2<>("valid_time_start", o.getTradeKey().getValidTimeStart()),
                                    new Tuple2<>("valid_time_end", o.getTradeKey().getValidTimeEnd()),
                                    new Tuple2<>("system_time_start", o.getSystemTimeStart()),
                                    new Tuple2<>("system_time_end", o.getSystemTimeEnd())));
        })
                .toList();

        final T object = objects.get(0);
        final Integer rowCount = new JdbcTemplate(dataSource).queryForObject(selectQuery
                .setFields(HashMap.of("count(*)", null))
                .setFilters(List.of(new SingleQueryFilter("id", QueryEquality.EQUALS, object.getTradeKey().getId())))
                .build(), Integer.class);
        if (rowCount > 0) {
            //update old ones end date
            System.out.println("need to update old row");
            new JdbcTemplate(dataSource).execute(new UpdateQuery<>(queryClass).addFields(List.of(new Tuple2<>("system_time_end", LocalDateTime.now())))
                    .addFilters(List.of(new SingleQueryFilter("id", QueryEquality.EQUALS, object.getTradeKey().getId())))
                    .build());
        }
        query.addFields(map);
        new JdbcTemplate(dataSource).execute(query.build());
        return this;
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

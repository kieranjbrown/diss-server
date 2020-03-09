package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import kieranbrown.bitemp.models.BitemporalModel;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.EntityManagerFactoryInfo;

import javax.persistence.Column;
import javax.persistence.EntityManager;
import javax.sql.DataSource;
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
        final DataSource dataSource = getDataSource(entityManager);
        final List<List<Tuple2<String, Object>>> map = objects.map(o -> {
            return List.of(queryClass.getDeclaredFields())
                    .map(x -> new Tuple2<>(x, getFieldName(x)))
                    .map(x -> new Tuple2<>(getColumnName(x._1), getFieldValue(x._2, o)))
                    .appendAll(
                            List.of(new Tuple2<>("id", o.getBitemporalKey().getId()),
                                    new Tuple2<>("valid_time_start", o.getBitemporalKey().getValidTimeStart()),
                                    new Tuple2<>("valid_time_end", o.getBitemporalKey().getValidTimeEnd()),
                                    new Tuple2<>("system_time_start", o.getSystemTimeStart()),
                                    new Tuple2<>("system_time_end", o.getSystemTimeEnd())));
        })
                .toList();

        final T object = objects.get(0);
        if (new JdbcTemplate(dataSource).queryForObject(selectQuery
                .setFields(HashMap.of("count(*)", null))
                .setFilters(List.of(new SingleQueryFilter("id", QueryEquality.EQUALS, object.getBitemporalKey().getId())))
                .build(), Integer.class) > 0) {
            //update old ones end date
            new JdbcTemplate(dataSource).execute(
                    new UpdateQuery<>(queryClass).addFields(List.of(new Tuple2<>("system_time_end", LocalDateTime.now())))
                            .addFilters(List.of(new SingleQueryFilter("id", QueryEquality.EQUALS, object.getBitemporalKey().getId())))
                            .build());
        }
        if (new SelectQueryBuilder<>(QueryType.SELECT, queryClass)
                .validTimeOverlaps(object.getBitemporalKey().getValidTimeStart(), object.getBitemporalKey().getValidTimeEnd())
                .execute(entityManager)
                .getResults()
                .length() > 0) {
            throw new OverlappingKeyException(String.format("overlapping valid time for id = '%s'", object.getBitemporalKey().getId()));
        }

        query.addFields(map);
        new JdbcTemplate(dataSource).execute(query.build());
        return this;
    }

    private DataSource getDataSource(EntityManager entityManager) {
        return ((EntityManagerFactoryInfo) entityManager.getEntityManagerFactory()).getDataSource();
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

package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import kieranbrown.bitemp.models.BitemporalModel;

import javax.persistence.Column;
import java.lang.reflect.Field;

public class QueryBuilder<T extends BitemporalModel<T>> {
    private final Query<T> query;
    private final Class<T> queryClass;

    private final List<String> baseFields = List.of(
            "id",
            "version",
            "valid_time_start",
            "valid_time_end",
            "system_time_start",
            "system_time_end");

    private QueryBuilder(final QueryType queryType, final Class<T> clazz) {
        queryClass = clazz;
        this.query = new Query<>(queryType, clazz);
    }

    public static <S extends BitemporalModel<S>> QueryBuilder<S> selectOne(final Class<S> clazz) {
        return new QueryBuilder<>(QueryType.SELECT_DISTINCT, clazz);
    }

    public static <S extends BitemporalModel<S>> QueryBuilder<S> selectMultiple(final Class<S> clazz) {
        return new QueryBuilder<>(QueryType.SELECT, clazz);
    }

    //TODO: other types of queries

    public QueryBuilder allFields() {
        query.setFields(HashMap.ofEntries(Stream.of(queryClass.getDeclaredFields())
                .map(this::getName)
                .insertAll(0, baseFields)
                .map(x -> new Tuple2<>(x, null))
        ));
        return this;
    }

    private String getName(final Field field) {
        final String annotationName = field.getAnnotation(Column.class).name();
        return "".equals(annotationName) ? field.getName() : annotationName;
    }

    public T getResult() {
        return null;
    }
}

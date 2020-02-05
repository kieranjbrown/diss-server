package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Stream;
import kieranbrown.bitemp.models.BitemporalModel;

import javax.persistence.Column;
import java.lang.reflect.Field;

public class QueryBuilder {
    private final Query query;
    private final Class<? extends BitemporalModel<?>> queryClass;

    private final List<String> baseFields = List.of(
            "id",
            "version",
            "valid_time_start",
            "valid_time_end",
            "system_time_start",
            "system_time_end");

    private QueryBuilder(final QueryType queryType, final Class<? extends BitemporalModel<?>> clazz) {
        queryClass = clazz;
        this.query = new Query(queryType, clazz);
    }

    public static QueryBuilder selectOne(final Class<? extends BitemporalModel<?>> clazz) {
        return new QueryBuilder(QueryType.SELECT_ONE, clazz);
    }

    public static QueryBuilder selectMultiple(final Class<? extends BitemporalModel<?>> clazz) {
        return new QueryBuilder(QueryType.SELECT_MANY, clazz);
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
}

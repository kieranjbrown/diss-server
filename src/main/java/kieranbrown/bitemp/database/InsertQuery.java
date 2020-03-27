package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import kieranbrown.bitemp.models.BitemporalModel;
import kieranbrown.bitemp.utils.QueryUtils;

import javax.persistence.Entity;

import static java.util.Objects.requireNonNull;

class InsertQuery<T extends BitemporalModel<T>> {
    private final Class<T> queryClass;
    private List<List<Tuple2<String, Object>>> fields;

    InsertQuery(final Class<T> clazz) {
        this.queryClass = requireNonNull(clazz, "class cannot be null");
        this.fields = List.empty();
    }

    InsertQuery<T> addFields(final List<List<Tuple2<String, Object>>> fields) {
        this.fields = this.fields.appendAll(fields);
        return this;
    }

    String build() {
        return "INSERT INTO " +
                getTableName() +
                " (" +
                getFields() +
                ") VALUES (" +
                getValues() +
                ")";
    }

    private String getTableName() {
        return queryClass.getAnnotation(Entity.class).name();
    }

    private Object getFields() {
        return fields.get(0).map(Tuple2::_1).mkString(", ");
    }

    private String getValues() {
        return fields.map(x -> x.map(Tuple2::_2).map(QueryUtils::toString).mkString(", "))
                .mkString("), (");
    }
}

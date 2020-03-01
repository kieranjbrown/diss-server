package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import kieranbrown.bitemp.models.BitemporalModel;
import kieranbrown.bitemp.utils.QueryUtils;

import javax.persistence.Entity;

import static java.util.Objects.requireNonNull;

class UpdateQuery<T extends BitemporalModel<T>> {
    private final Class<T> queryClass;
    private List<Tuple2<String, Object>> fields;
    private List<QueryFilter> filters;

    UpdateQuery(final Class<T> clazz) {
        this.queryClass = requireNonNull(clazz, "class cannot be null");
        this.fields = List.empty();
        this.filters = List.empty();
    }

    UpdateQuery<T> addFields(final List<Tuple2<String, Object>> fields) {
        this.fields = this.fields.appendAll(fields);
        return this;
    }

    UpdateQuery<T> addFilters(final List<QueryFilter> filters) {
        this.filters = this.filters.appendAll(filters);
        return this;
    }

    String build() {
        return "UPDATE " +
                getTableName() +
                " SET " +
                getFieldsToUpdate() +
                " WHERE " +
                getFilters();
    }

    private String getTableName() {
        return queryClass.getAnnotation(Entity.class).name();
    }

    private String getFieldsToUpdate() {
        return fields.map(x -> x.map2(QueryUtils::toString))
                .map(x -> x._1 + " = " + x._2)
                .reduce((x, y) -> x + ", " + y);
    }

    private String getFilters() {
        return filters.map(QueryFilter::getFilters)
                .reduce((x, y) -> x + " AND " + y);
    }
}

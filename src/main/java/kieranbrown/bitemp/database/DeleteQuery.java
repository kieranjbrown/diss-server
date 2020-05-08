package kieranbrown.bitemp.database;

import io.vavr.collection.List;
import kieranbrown.bitemp.models.BitemporalModel;

import javax.persistence.Entity;

import static java.util.Objects.requireNonNull;

class DeleteQuery<T extends BitemporalModel<T>> {
    private final Class<T> queryClass;
    private List<QueryFilter> filters;

    DeleteQuery(final Class<T> clazz) {
        this.queryClass = requireNonNull(clazz, "class cannot be null");
        this.filters = List.empty();
    }

    DeleteQuery<T> addFilters(final List<QueryFilter> filters) {
        this.filters = this.filters.appendAll(filters);
        return this;
    }

    String build() {
        return "DELETE FROM " +
                getTableName() +
                " WHERE " +
                getFilters();
    }

    private String getTableName() {
        return queryClass.getAnnotation(Entity.class).name();
    }

    private String getFilters() {
        return filters.map(QueryFilter::getFilters)
                .mkString(" AND ");
    }
}

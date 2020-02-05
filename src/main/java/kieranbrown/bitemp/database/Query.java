package kieranbrown.bitemp.database;

import io.vavr.collection.Map;
import kieranbrown.bitemp.models.BitemporalModel;
import org.apache.commons.lang3.builder.EqualsBuilder;

class Query {
    private final QueryType queryType;
    private final Class<? extends BitemporalModel<?>> queryClass;
    private Map<String, Object> fields;

    public Query(final QueryType queryType, final Class<? extends BitemporalModel<?>> clazz) {
        this.queryType = queryType;
        queryClass = clazz;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        Query query = (Query) o;

        return new EqualsBuilder()
                .append(queryType, query.queryType)
                .append(queryClass, query.queryClass)
                .append(fields, query.fields)
                .isEquals();
    }

    public void setFields(final Map<String, Object> fields) {
        this.fields = fields;
    }
}

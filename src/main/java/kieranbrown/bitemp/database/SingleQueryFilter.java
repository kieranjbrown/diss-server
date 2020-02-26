package kieranbrown.bitemp.database;

import io.vavr.Tuple3;
import org.apache.commons.lang3.Validate;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import static java.util.Objects.requireNonNull;

//TODO: make interface, some contain single filter and some contain AND / OR ?
public class SingleQueryFilter implements QueryFilter {
    private final Tuple3<String, QueryEquality, Object> filter;

    public SingleQueryFilter(final Tuple3<String, QueryEquality, Object> filter) {
        this.filter = requireNonNull(filter, "filter cannot be null");
    }

    public SingleQueryFilter(final String column, final QueryEquality condition, final Object value) {
        this.filter = new Tuple3<>(
                Validate.notBlank(column, "column cannot be null/blank"),
                requireNonNull(condition, "condition cannot be null"),
                requireNonNull(value, "value cannot be null")
        );
    }

    public String getFilters() {
        //TODO: the format string to SQL method in Query needs to be moved elsewhere, or this method needs moved
        return filter.apply((x, y, z) -> String.format("%s %s %s", x, y.getValue(), z));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        SingleQueryFilter that = (SingleQueryFilter) o;

        return new EqualsBuilder()
                .append(filter, that.filter)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(filter)
                .toHashCode();
    }
}

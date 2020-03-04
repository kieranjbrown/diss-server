package kieranbrown.bitemp.database;

import io.vavr.Tuple2;
import io.vavr.collection.List;
import kieranbrown.bitemp.models.BitemporalModel;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

import static java.util.Objects.requireNonNull;

public class UpdateQueryBuilder<T extends BitemporalModel<T>> {
    private final Class<T> queryClass;
    private List<Tuple2<String, Object>> fields;
    private List<QueryFilter> filters;

    public UpdateQueryBuilder(final Class<T> clazz) {
        queryClass = requireNonNull(clazz, "class cannot be null");
        this.fields = List.empty();
        this.filters = List.empty();
    }

    public UpdateQueryBuilder<T> set(final String column, final Object value) {
        this.fields = this.fields.append(new Tuple2<>(column, value));
        return this;
    }

    public UpdateQueryBuilder<T> where(final QueryFilter queryFilter) {
        this.filters = this.filters.append(queryFilter);
        return this;
    }

//    public UpdateQueryBuilder<T> forValidTimePeriod(final )

    public void execute(final DataSource dataSource) {
        final String sql = new UpdateQuery<>(queryClass)
                .addFields(fields)
                .addFilters(filters)
                .build();

        new JdbcTemplate(dataSource).execute(sql);
    }
}

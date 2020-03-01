package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.BitemporalModel;

import javax.persistence.EntityManager;

public class InsertQueryBuilder<T extends BitemporalModel<T>> {
    private final QueryType queryType;
    private final Class<T> queryClass;
    private final Query<T> query;

    InsertQueryBuilder(final QueryType queryType, final Class<T> queryClass) {
        this.queryType = queryType;
        this.queryClass = queryClass;
        this.query = new Query<>(queryType, queryClass);
    }

    public InsertQueryBuilder<T> from(final T object) {
        return this;
    }

    public InsertQueryBuilder<T> execute(final EntityManager entityManager) {
        return this;
    }
}

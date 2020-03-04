package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.BitemporalModel;

import static java.util.Objects.requireNonNull;

public final class QueryBuilderFactory {

    private QueryBuilderFactory() {
    }

    public static <S extends BitemporalModel<S>> SelectQueryBuilder<S> selectDistinct(final Class<S> clazz) {
        return new SelectQueryBuilder<>(QueryType.SELECT_DISTINCT, clazz);
    }

    public static <S extends BitemporalModel<S>> SelectQueryBuilder<S> select(final Class<S> clazz) {
        return new SelectQueryBuilder<>(QueryType.SELECT, clazz);
    }

    //TODO: other types of queries
    public static <S extends BitemporalModel<S>> InsertQueryBuilder<S> insert(final Class<S> clazz) {
        return new InsertQueryBuilder<>(requireNonNull(clazz, "class cannot be null"));
    }

    public static <S extends BitemporalModel<S>> UpdateQueryBuilder<S> update(final Class<S> clazz) {
        return new UpdateQueryBuilder<>(requireNonNull(clazz, "class cannot be null"));
    }
}

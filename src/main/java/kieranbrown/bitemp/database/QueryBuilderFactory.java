package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.BitemporalModel;

import static java.util.Objects.requireNonNull;

public final class QueryBuilderFactory {

    private QueryBuilderFactory() {
    }

    public static <S extends BitemporalModel<S>> SelectQueryBuilder<S> select(final Class<S> clazz) {
        return new SelectQueryBuilder<>(clazz);
    }

    public static <S extends BitemporalModel<S>> InsertQueryBuilder<S> insert(final Class<S> clazz) {
        return new InsertQueryBuilder<>(requireNonNull(clazz, "class cannot be null"));
    }

    public static <S extends BitemporalModel<S>> UpdateQueryBuilder<S> update(final Class<S> clazz) {
        return new UpdateQueryBuilder<>(requireNonNull(clazz, "class cannot be null"));
    }

    public static <S extends BitemporalModel<S>> DeleteQueryBuilder<S> delete(final Class<S> clazz) {
        return new DeleteQueryBuilder<>(requireNonNull(clazz, "class cannot be null"));
    }
}

package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.BitemporalModel;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.Repository;

import java.util.List;

@NoRepositoryBean
public interface BitemporalWriteRepository<T extends BitemporalModel<T>> extends Repository<T, BitemporalKey> {
    void save(T t);

    default void saveAll(List<T> t) {
        t.forEach(this::save);
    }
}
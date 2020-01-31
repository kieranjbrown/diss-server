package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.Trade;
import org.springframework.data.jpa.repository.Query;

import java.util.List;
import java.util.UUID;

//TODO: split these into read, write etc repos then have a service combinbing them to handle all ops?
public interface TradeRepository extends BitemporalRepository<Trade> {
    @Query(value = "select * from #{#entityName} t where t.id = ?1 and t.system_time_end >= '9999-12-31'", nativeQuery = true)
    Trade findMostRecent(UUID id);

    @Query(value = "select * from reporting.trade_data t where t.id = ?1 order by version asc", nativeQuery = true)
    List<Trade> findAllById(UUID id);
}

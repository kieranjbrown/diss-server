package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.Trade;
import org.springframework.data.jpa.repository.Query;

import java.util.Date;
import java.util.List;
import java.util.UUID;

//TODO: split these into read, write etc repos then have a service combining them to handle all ops?
public interface TradeRepository extends BitemporalRepository<Trade> {
    @Query(value = "select * from #{#entityName} t where t.id = ?1 and t.system_time_end >= '9999-12-31'", nativeQuery = true)
    Trade findMostRecent(UUID id);

    @Query(value = "select * from #{#entityName} t where t.id = ?1 order by version asc", nativeQuery = true)
    List<Trade> findAllById(UUID id);

    //equivalent to BETWEEN x and y (inclusive on both sides)
    @Query(value = "select * from #{#entityName} t where t.system_time_start >= ?1 and t.system_time_end <= ?2", nativeQuery = true)
    List<Trade> findAllBetweenSystemTimeRange(Date startRange, Date endRange);

    //equivalent to AS OF x
    @Query(value = "select * from #{#entityName} t where t.system_time_start <= ?1 and t.system_time_end > ?1", nativeQuery = true)
    List<Trade> findAllAsOfSystemTime(Date systemTime);

    //equivalent to FROM x to y (inclusive / exclusive)
    @Query(value = "select * from #{#entityName} t where t.system_time_start >= ?1 and t.system_time_end < ?2", nativeQuery = true)
    List<Trade> findAllFromSystemTimeRange(Date startTime, Date endTime);
}

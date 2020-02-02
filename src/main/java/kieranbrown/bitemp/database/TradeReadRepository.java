package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.Trade;
import org.springframework.data.jpa.repository.Query;

import java.time.LocalDate;
import java.util.Date;
import java.util.List;
import java.util.UUID;

//TODO: split these into read, write etc repos then have a service combining them to handle all ops?
public interface TradeReadRepository extends BitemporalRepository<Trade> {
    @Query(value = "select * from #{#entityName} t where t.id = ?1 order by version asc", nativeQuery = true)
    List<Trade> findAllById(UUID id);

    /*
     * SYSTEM TIME METHODS
     * These methods are implemented in accordance with the SQL:2011 specification, discussed at
     * http://cs.unibo.it/~montesi/CBD/Articoli/Temporal%20features%20in%20SQL2011.pdf
     */

    @Query(value = "select * from #{#entityName} t where t.id = ?1 and t.system_time_end >= '9999-12-31'", nativeQuery = true)
    Trade findMostRecentBySystemTime(UUID id);

    //equivalent to BETWEEN x and y (inclusive on both sides)
    @Query(value = "select * from #{#entityName} t where t.system_time_start >= ?1 and t.system_time_end <= ?2", nativeQuery = true)
    List<Trade> findAllBetweenSystemTimeRange(Date startTime, Date endTime);

    //equivalent to AS OF x (system time) 
    @Query(value = "select * from #{#entityName} t where t.system_time_start <= ?1 and t.system_time_end > ?1", nativeQuery = true)
    List<Trade> findAllAsOfSystemTime(Date systemTime);

    //equivalent to FROM x to y (inclusive / exclusive)
    @Query(value = "select * from #{#entityName} t where t.system_time_start >= ?1 and t.system_time_end < ?2", nativeQuery = true)
    List<Trade> findAllFromSystemTimeRange(Date startTime, Date endTime);

    /*
     * VALID TIME METHODS
     * These methods are implemented in accordance with the SQL:2011 specification, discussed at
     * http://cs.unibo.it/~montesi/CBD/Articoli/Temporal%20features%20in%20SQL2011.pdf and explained further at
     * https://docs.sqlstream.com/sql-reference-guide/temporal-predicates/ and
     * http://wwwlgis.informatik.uni-kl.de/cms/fileadmin/courses/SS2014/Neuere_Entwicklungen/Chapter_10_-_Temporal_DM.pdf
     * */

    //equivalent to CONTAINS x (application time)
    //TODO: logic needs refining as per above links
    @Query(value = "select * from #{#entityName} t where t.valid_time_start <= ?1 and t.valid_time_end > ?1", nativeQuery = true)
    List<Trade> findAllContainsValidTime(LocalDate validTime);

    //equivalent to OVERLAPS x and y (exclusive / exclusive)
    //TODO: logic needs updating as per above logic
    @Query(value = "select * from #{#entityName} t where t.valid_time_start < ?1 and t.valid_time_end > ?2", nativeQuery = true)
    List<Trade> findAllOverlappingValidTimeRange(LocalDate startDate, LocalDate endDate);

    //equivalent to x equals y
    @Query(value = "select * from #{#entityName} t where t.valid_time_start = ?1 and t.valid_time_end = ?2", nativeQuery = true)
    List<Trade> findAllWhereValidTimeIsEqual(LocalDate startDate, LocalDate endDate);

    //equivalent to x precedes y
    @Query(value = "select * from #{#entityName} t where t.valid_time_start < ?1 and t.valid_time_end <= ?2", nativeQuery = true)
    List<Trade> findAllWhereValidTimeIsPreceded(LocalDate startDate, LocalDate endDate);

    //equivalent to x immediately precedes y
    @Query(value = "select * from #{#entityName} t where t.valid_time_start < ?1 and t.valid_time_end = ?2", nativeQuery = true)
    List<Trade> findAllWhereValidTimeIsImmediatelyPreceded(LocalDate startDate, LocalDate endDate);

    //equivalent to x succeeds y
    @Query(value = "select * from #{#entityName} t where t.valid_time_start >= ?1", nativeQuery = true)
    List<Trade> findAllWhereValidTimeIsSucceeded(LocalDate endDate);

    //equivalent to x immediately succeeds y
    @Query(value = "select * from #{#entityName} t where t.valid_time_start = ?1", nativeQuery = true)
    List<Trade> findAllWhereValidTimeIsImmediatelySucceeded(LocalDate endDate);

    /*
     * BITEMPORAL METHODS
     * these are a combination of each of the above methods for filtering on both dimensions
     * */
}

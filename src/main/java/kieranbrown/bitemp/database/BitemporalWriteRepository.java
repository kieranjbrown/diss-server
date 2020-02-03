package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.Trade;
import org.springframework.data.jpa.repository.JpaRepository;

//TODO: change this to not be a full JPA repository
public interface BitemporalWriteRepository extends JpaRepository<Trade, BitemporalKey> {

}

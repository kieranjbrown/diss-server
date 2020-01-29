package kieranbrown.bitemp.database;

import kieranbrown.bitemp.models.BitemporalKey;
import kieranbrown.bitemp.models.BitemporalModel;
import org.springframework.data.jpa.repository.JpaRepository;

//TODO: rip all this abstract / interface stuff out into a separate package
interface BitemporalRepository<T extends BitemporalModel> extends JpaRepository<T, BitemporalKey> {
}

package kieranbrown.bitemp;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication(scanBasePackages = {"kieranbrown.bitemp"})
public class ServerApplication {

	public static void main(final String[] args) {
		SpringApplication.run(ServerApplication.class, args);
	}

}

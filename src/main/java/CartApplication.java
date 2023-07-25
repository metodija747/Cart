import com.kumuluz.ee.discovery.annotations.RegisterService;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.logging.Logger;

@ApplicationPath("/")
@RegisterService
public class CartApplication extends Application {
    private static final Logger LOG = Logger.getLogger(CartApplication.class.getName());

    public CartApplication() {
        LOG.info("CartApplication started!");
    }
}



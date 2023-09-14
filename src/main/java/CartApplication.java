import com.kumuluz.ee.discovery.annotations.RegisterService;
import org.eclipse.microprofile.auth.LoginConfig;
import org.eclipse.microprofile.openapi.annotations.OpenAPIDefinition;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.eclipse.microprofile.openapi.annotations.info.Info;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.logging.Logger;

@ApplicationPath("/")
@LoginConfig(authMethod = "MP-JWT")
@RegisterService
@OpenAPIDefinition(
        info = @Info(title = "User's cart API", version = "1.0.0"),
        security = @SecurityRequirement(name = "jwtAuth")
)
@SecurityScheme(
        securitySchemeName = "jwtAuth",
        type = SecuritySchemeType.HTTP,
        scheme = "bearer",
        bearerFormat = "JWT"
)
public class CartApplication extends Application {
    private static final Logger LOG = Logger.getLogger(CartApplication.class.getName());

    public CartApplication() {
        LOG.info("CartApplication started!");
    }
}



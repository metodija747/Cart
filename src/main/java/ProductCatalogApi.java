import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/products")
@RegisterRestClient
public interface ProductCatalogApi {

    @GET
    @Path("/{productId}")
    @Produces(MediaType.APPLICATION_JSON)
    Response getProduct(@PathParam("productId") String productId);
}

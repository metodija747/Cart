import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.auth0.jwk.JwkException;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.kumuluz.ee.discovery.annotations.DiscoverService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.logging.Logger;

@Path("/cart")
public class CartResource {

    @Inject
    @DiscoverService(value = "catalog-service", environment = "dev", version = "1.0.0")
    private Optional<URL> productCatalogUrl;

    @Inject
    @DiscoverService(value = "orders-service", environment = "dev", version = "1.0.0")
    private Optional<URL> ordersUrl;


    private DynamoDbClient dynamoDB = DynamoDbClient.builder()
            .region(Region.US_EAST_1)
            .build();
    private String tableName = "CartDB";
    String issuer = "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_cl8iVMzUw";
    private static final Logger LOGGER = Logger.getLogger(CartResource.class.getName());
    private static final int PAGE_SIZE = 3;

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public Response getCart(@HeaderParam("Auth") String token,
                            @QueryParam("page") Integer page) {
        LOGGER.info("DynamoDB response: " + productCatalogUrl);
        String userId;
        try {
            userId = TokenVerifier.verifyToken(token, issuer);
        } catch (JWTVerificationException | JwkException | MalformedURLException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
        LOGGER.info(userId);
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":v_id", AttributeValue.builder().s(userId).build());

        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("UserId = :v_id")
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        try {
            QueryResponse queryResponse = dynamoDB.query(queryRequest);
            List<Map<String, AttributeValue>> itemsList = queryResponse.items();

            if (itemsList.isEmpty()) {
                return Response.status(Response.Status.NOT_FOUND).entity("No items found in cart.").build();
            }
            Map<String, AttributeValue> userCart = itemsList.get(0);
            List<Map<String, String>> items = ResponseTransformer.transformCartItems(Collections.singletonList(userCart));
            List<Map<String, String>> products = new Gson().fromJson(items.get(0).get("products"), new TypeToken<List<Map<String, String>>>() {
            }.getType());
            int totalPages = (int) Math.ceil((double) products.size() / PAGE_SIZE);
            if (page == null) {
                page = 1;
            }

            int start = (page - 1) * PAGE_SIZE;
            int end = Math.min(start + PAGE_SIZE, products.size());
            List<Map<String, String>> pagedProducts = products.subList(start, end);

            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("products", pagedProducts);
            responseBody.put("totalPages", totalPages);
            responseBody.put("totalPrice", items.get(0).get("TotalPrice"));

            return Response.ok(responseBody).build();
        } catch (DynamoDbException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }


    @POST
    @Path("/add")
    public Response addToCart(@HeaderParam("Auth") String token, CartItem cartItem) {
        String userId;
        try {
            userId = TokenVerifier.verifyToken(token, issuer);
        } catch (JWTVerificationException | JwkException | MalformedURLException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }

        // Extract the productId from the cartItem
        String productId = cartItem.getProductId();
        String quantity = cartItem.getQuantity();

        // Construct the key for the item
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("UserId", AttributeValue.builder().s(userId).build());

        // Define a GetItemRequest
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build();

        try {
            // Send the GetItemRequest
            GetItemResponse getItemResponse = dynamoDB.getItem(getItemRequest);

            // Check if the user exists in the table, if not, create an empty OrderList and initialize TotalPrice
            if (getItemResponse.item() == null || !getItemResponse.item().containsKey("OrderList") || !getItemResponse.item().containsKey("TotalPrice")) {
                Map<String, String> expressionAttributeNames = new HashMap<>();
                expressionAttributeNames.put("#O", "OrderList");
                expressionAttributeNames.put("#T", "TotalPrice");

                Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
                expressionAttributeValues.put(":ol", AttributeValue.builder().s(";").build()); // create an empty OrderList
                expressionAttributeValues.put(":t", AttributeValue.builder().n("0.0").build()); // initialize TotalPrice

                UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(key)
                        .updateExpression("SET #O = :ol, #T = :t")
                        .expressionAttributeNames(expressionAttributeNames)
                        .expressionAttributeValues(expressionAttributeValues)
                        .build();
                dynamoDB.updateItem(updateItemRequest);

                // Re-fetch the item after creating an empty OrderList and initializing TotalPrice
                getItemResponse = dynamoDB.getItem(getItemRequest);
            }
            // Get the OrderList
            String orderListStr = getItemResponse.item().get("OrderList").s();
            if (!orderListStr.endsWith(";")) {
                orderListStr += ";";
            }
            String[] orderList = orderListStr.split(";");

            // Parse the OrderList and look for the product
            boolean found = false;
            for (int i = 0; i < orderList.length; i++) {
                String[] parts = orderList[i].split(":");
                if (parts[0].equals(productId)) {
                    // Found the product, update the quantity
                    parts[1] = String.valueOf(Integer.parseInt(quantity));
                    orderList[i] = String.join(":", parts);
                    found = true;
                    break;
                }
            }

            // If the product was not found, add it to the end of the OrderList
            if (!found) {
                String[] newOrderList = Arrays.copyOf(orderList, orderList.length + 1);
                newOrderList[newOrderList.length - 1] = productId + ":" + quantity;
                orderList = newOrderList;
            }

            // Convert the OrderList back to a string
            orderListStr = String.join(";", orderList);

            // Prepare the expression attributes
            Map<String, String> expressionAttributeNames = new HashMap<>();
            expressionAttributeNames.put("#O", "OrderList");
            Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
            expressionAttributeValues.put(":ol", AttributeValue.builder().s(orderListStr).build());
            UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .updateExpression("SET #O = :ol")
                    .expressionAttributeNames(expressionAttributeNames)
                    .expressionAttributeValues(expressionAttributeValues)
                    .build();
            dynamoDB.updateItem(updateItemRequest);

            double totalPrice = 0.0;
            Gson gson = new Gson();
            Client client = ClientBuilder.newClient();
            for (String order : orderList) {
                String[] parts = order.split(":");
                String productIdOrder = parts[0];
                int quantityOrder = Integer.parseInt(parts[1]);
                // Call the product catalog microservice to get the product details
                if (productCatalogUrl.isPresent()) {
                    WebTarget target = client.target(productCatalogUrl.get().toString() + "/products/" + productIdOrder);
                    Response response = target.request(MediaType.APPLICATION_JSON).get();

                    if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                        throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
                    }

                    String json = response.readEntity(String.class);
                    Product product = gson.fromJson(json, Product.class);

                    // Get the price of the product
                    double productPrice = product.getPrice();

                    // Add the total price of this product to the total price
                    totalPrice += productPrice * quantityOrder;
                }
            }

                // Update the TotalPrice in the CartDB table
                Map<String, String> expressionAttributeNamesTotal = new HashMap<>();
                expressionAttributeNamesTotal.put("#T", "TotalPrice");

                Map<String, AttributeValue> expressionAttributeValuesTotal = new HashMap<>();
                expressionAttributeValuesTotal.put(":t", AttributeValue.builder().n(String.valueOf(totalPrice)).build());

                UpdateItemRequest updateTotalPriceRequest = UpdateItemRequest.builder()
                        .tableName(tableName)
                        .key(key)
                        .updateExpression("SET #T = :t")
                        .expressionAttributeNames(expressionAttributeNamesTotal)
                        .expressionAttributeValues(expressionAttributeValuesTotal)
                        .build();

                dynamoDB.updateItem(updateTotalPriceRequest);

                // Fetch the updated item
                GetItemResponse updatedItemResponse = dynamoDB.getItem(getItemRequest);

                return Response.ok(ResponseTransformer.transformItem(updatedItemResponse.item())).build();
        } catch (DynamoDbException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.awsErrorDetails().errorMessage()).build();
        }
    }



    @DELETE
    @Path("/{productId}")
    public Response deleteFromCart(@HeaderParam("Auth") String token, @PathParam("productId") String productId) {
        String userId;
        LOGGER.info("DynamoDB response: " + productCatalogUrl);
        try {
            userId = TokenVerifier.verifyToken(token, issuer);
        } catch (JWTVerificationException | JwkException | MalformedURLException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }

        // Construct the key for the item
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("UserId", AttributeValue.builder().s(userId).build());

        // Define a GetItemRequest
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build();

        try {
            // Send the GetItemRequest
            GetItemResponse getItemResponse = dynamoDB.getItem(getItemRequest);

            // Get the OrderList
            String orderListStr = getItemResponse.item().get("OrderList").s();
            double totalPrice = Double.parseDouble(getItemResponse.item().get("TotalPrice").n());
            // Split the OrderList by ";" and filter out the product to delete
            String[] orderList = orderListStr.split(";");
            StringBuilder updatedOrderListStr = new StringBuilder();
            int quantityToDelete = 0;
            for (String order : orderList) {
                String[] parts = order.split(":");
                if (parts[0].equals(productId)) {
                    quantityToDelete = Integer.parseInt(parts[1]);
                } else {
                    updatedOrderListStr.append(order).append(";");
                }
            }
            Client client = ClientBuilder.newClient();
            Gson gson = new Gson();
            WebTarget target = client.target(productCatalogUrl.get().toString() + "/products/" + productId);
            Response response = target.request(MediaType.APPLICATION_JSON).get();

            if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
            }

            String json = response.readEntity(String.class);
            Product product = gson.fromJson(json, Product.class);

            // Get the price of the product
            double productPrice = product.getPrice();

            // Add the total price of this product to the total price
            double updatedTotalPrice = totalPrice - productPrice * quantityToDelete;

            // Create the updated OrderList and TotalPrice attributes
            AttributeValue updatedOrderListAttr = AttributeValue.builder().s(updatedOrderListStr.toString()).build();
            AttributeValue updatedTotalPriceAttr = AttributeValue.builder().n(String.valueOf(updatedTotalPrice)).build();

            // Define the updated item attributes
            Map<String, AttributeValueUpdate> updatedItemAttrs = new HashMap<>();
            updatedItemAttrs.put("OrderList", AttributeValueUpdate.builder().value(updatedOrderListAttr).action(AttributeAction.PUT).build());
            updatedItemAttrs.put("TotalPrice", AttributeValueUpdate.builder().value(updatedTotalPriceAttr).action(AttributeAction.PUT).build());

            // Define the UpdateItemRequest
            UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                    .tableName(tableName)
                    .key(key)
                    .attributeUpdates(updatedItemAttrs)
                    .build();

            // Update the item
            dynamoDB.updateItem(updateItemRequest);
            // Create a response map
            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("TotalPrice", updatedTotalPrice);
            responseBody.put("message", "Product deleted from cart successfully");
            return Response.ok(responseBody).build();
        } catch (DynamoDbException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

    @DELETE
    public Response deleteFromCart(@HeaderParam("Auth") String token) {
        String userId;
        try {
            userId = TokenVerifier.verifyToken(token, issuer);
        } catch (JWTVerificationException | JwkException | MalformedURLException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }

        // Construct the key for the item
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("UserId", AttributeValue.builder().s(userId).build());

        // Define a DeleteItemRequest
        DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build();

        try {
            // Delete the item
            dynamoDB.deleteItem(deleteItemRequest);

            // Create a response map
            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("message", "Cart deleted successfully");
            return Response.ok(responseBody).build();
        } catch (DynamoDbException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

}

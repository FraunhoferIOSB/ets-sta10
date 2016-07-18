package org.opengis.cite.sta10.sensingCore;

import java.util.Map;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.cite.sta10.SuiteAttribute;
import org.opengis.cite.sta10.util.EntityType;
import org.opengis.cite.sta10.util.HTTPMethods;
import org.opengis.cite.sta10.util.ServiceURLBuilder;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Creates entities for tests of "A.1 Sensing Core" Conformance class.
 */
public class Capability0Tests {

    /**
     * The root URL of the SensorThings service under the test
     */
    public String rootUri;

    /**
     * This method will be run before starting the test for this conformance
     * class.
     *
     * @param testContext The test context to find out whether this class is
     * requested to test or not
     */
    @BeforeClass
    public void obtainTestSubject(ITestContext testContext) {
        Object obj = testContext.getSuite().getAttribute(
                SuiteAttribute.LEVEL.getName());
        if ((null != obj)) {
            Integer level = Integer.class.cast(obj);
            Assert.assertTrue(level.intValue() >= 0,
                    "Conformance level 1 will not be checked since ics = " + level);
        }

        rootUri = testContext.getSuite().getAttribute(
                SuiteAttribute.TEST_SUBJECT.getName()).toString();
        rootUri = rootUri.trim();
        if (rootUri.lastIndexOf('/') == rootUri.length() - 1) {
            rootUri = rootUri.substring(0, rootUri.length() - 1);
        }

        // Check if there is data to test on. We check Observation and
        // HistoricalLocation, since if those exist, all other entities should
        // also exist.
        String responseObservations = getEntities(EntityType.OBSERVATION);
        String responseHistLocations = getEntities(EntityType.HISTORICAL_LOCATION);
        int countObservations = countEntitiesInResponse(responseObservations);
        int countHistLocations = countEntitiesInResponse(responseHistLocations);
        if (countHistLocations == 0 || countObservations == 0) {
            // No data found, insert test data.
            createTestEntities();
        }
    }

    private void createTestEntities() {
        String urlParameters = "{\n"
                + "    \"name\": \"thing 1\",\n"
                + "    \"description\": \"thing 1\",\n"
                + "    \"properties\": {\n"
                + "        \"reference\": \"first\"\n"
                + "    },\n"
                + "    \"Locations\": [\n"
                + "        {\n"
                + "            \"name\": \"location 1\",\n"
                + "            \"description\": \"location 1\",\n"
                + "            \"location\": {\n"
                + "                \"type\": \"Point\",\n"
                + "                \"coordinates\": [\n"
                + "                    -117.05,\n"
                + "                    51.05\n"
                + "                ]\n"
                + "            },\n"
                + "            \"encodingType\": \"application/vnd.geo+json\"\n"
                + "        }\n"
                + "    ],\n"
                + "    \"Datastreams\": [\n"
                + "        {\n"
                + "            \"unitOfMeasurement\": {\n"
                + "                \"name\": \"Lumen\",\n"
                + "                \"symbol\": \"lm\",\n"
                + "                \"definition\": \"http://www.qudt.org/qudt/owl/1.0.0/unit/Instances.html#Lumen\"\n"
                + "            },\n"
                + "            \"name\": \"datastream 1\",\n"
                + "            \"description\": \"datastream 1\",\n"
                + "            \"observationType\": \"http://www.opengis.net/def/observationType/OGC-OM/2.0/OM_Measurement\",\n"
                + "            \"ObservedProperty\": {\n"
                + "                \"name\": \"Luminous Flux\",\n"
                + "                \"definition\": \"http://www.qudt.org/qudt/owl/1.0.0/quantity/Instances.html#LuminousFlux\",\n"
                + "                \"description\": \"observedProperty 1\"\n"
                + "            },\n"
                + "            \"Sensor\": {\n"
                + "                \"name\": \"sensor 1\",\n"
                + "                \"description\": \"sensor 1\",\n"
                + "                \"encodingType\": \"http://schema.org/description\",\n"
                + "                \"metadata\": \"Light flux sensor\"\n"
                + "            },\n"
                + "            \"Observations\": [{\n"
                + "                \"phenomenonTime\": \"2015-03-01T00:00:00Z\",\n"
                + "                \"result\": 1, \n"
                + "                \"FeatureOfInterest\": {\n"
                + "                    \"name\": \"Some Feature\",\n"
                + "                    \"description\": \"Some Feature\",\n"
                + "                    \"encodingType\": \"application/vnd.geo+json\",\n"
                + "                    \"feature\": {\n"
                + "                        \"type\": \"Feature\",\n"
                + "                        \"geometry\": {\n"
                + "                            \"type\": \"Polygon\",\n"
                + "                            \"coordinates\": [\n"
                + "                                [[100,50], [10,9], [23,4], [100,50]], [[30,20], [10,4], [4,22], [30,20]]\n"
                + "                            ]\n"
                + "                        }"
                + "                    }"
                + "                }"
                + "            }]"
                + "        }\n"
                + "    ]\n"
                + "}";
        String urlString = ServiceURLBuilder.buildURLString(rootUri, EntityType.THING, -1, null, null);
        Map<String, Object> responseMap = HTTPMethods.doPost(urlString, urlParameters);
    }

    /**
     * This helper method is sending GET request to a collection of entities.
     *
     * @param entityType Entity type from EntityType enum list
     * @return The response of GET request in string format.
     */
    private String getEntities(EntityType entityType) {
        String urlString = rootUri;
        if (entityType != null) {
            urlString = ServiceURLBuilder.buildURLString(rootUri, entityType, -1, null, null);
        }
        Map<String, Object> responseMap = HTTPMethods.doGet(urlString);
        String response = responseMap.get("response").toString();
        int responseCode = Integer.parseInt(responseMap.get("response-code").toString());
        Assert.assertEquals(responseCode, 200, "Error during getting entities: " + ((entityType != null) ? entityType.name() : "root URI"));
        if (entityType != null) {
            Assert.assertTrue(response.indexOf("value") != -1, "The GET entities response for entity type \"" + entityType + "\" does not match SensorThings API : missing \"value\" in response.");
        } else { // GET Service Base URI
            Assert.assertTrue(response.indexOf("value") != -1, "The GET entities response for service root URI does not match SensorThings API : missing \"value\" in response.");
        }
        return response.toString();
    }

    private int countEntitiesInResponse(String response) {
        try {
            JSONObject jsonResponse = new JSONObject(response);
            JSONArray entities = jsonResponse.getJSONArray("value");
            return entities.length();
        } catch (JSONException e) {
            e.printStackTrace();
            Assert.fail("An Exception occurred during testing!:\n" + e.getMessage());
        }
        return 0;
    }

    @Test(description = "GET Entities", groups = "level-0", priority = -1)
    public void emptyTest() {
        // We do not actually test anything here.
    }
}

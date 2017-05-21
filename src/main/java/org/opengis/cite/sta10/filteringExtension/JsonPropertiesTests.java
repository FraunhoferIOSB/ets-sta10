package org.opengis.cite.sta10.filteringExtension;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.geojson.Point;
import org.opengis.cite.sta10.SuiteAttribute;
import org.opengis.cite.sta10.util.EntityUtils;
import org.opengis.cite.sta10.util.HTTPMethods;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for the geospatial functions.
 *
 * @author Hylke van der Schaaf
 */
public class JsonPropertiesTests {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(JsonPropertiesTests.class);
    private String rootUri;
    private SensorThingsService service;
    private final List<Thing> THINGS = new ArrayList<>();
    private final List<Location> LOCATIONS = new ArrayList<>();
    private final List<Sensor> SENSORS = new ArrayList<>();
    private final List<ObservedProperty> O_PROPS = new ArrayList<>();
    private final List<Datastream> DATASTREAMS = new ArrayList<>();
    private final List<Observation> OBSERVATIONS = new ArrayList<>();

    public JsonPropertiesTests() {
    }

    @BeforeClass()
    public void setUp(ITestContext testContext) {
        LOGGER.info("Setting up class.");
        Object obj = testContext.getSuite().getAttribute(
                SuiteAttribute.LEVEL.getName());
        if ((null != obj)) {
            Integer level = Integer.class.cast(obj);
            Assert.assertTrue(level.intValue() > 2,
                    "Conformance level 3 will not be checked since ics = " + level);
        }

        rootUri = testContext.getSuite().getAttribute(
                SuiteAttribute.TEST_SUBJECT.getName()).toString();
        rootUri = rootUri.trim();
        if (rootUri.lastIndexOf('/') == rootUri.length() - 1) {
            rootUri = rootUri.substring(0, rootUri.length() - 1);
        }
        URL url;
        try {
            url = new URL(rootUri);
            service = new SensorThingsService(url);
            createEntities();
        } catch (MalformedURLException | URISyntaxException ex) {
            LOGGER.error("Failed to create service uri.", ex);
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to create entities.", ex);
        } catch (Exception ex) {
            LOGGER.error("Unknown Exception.", ex);
        }
    }

    @AfterClass
    public void tearDown() {
        LOGGER.info("tearing down class.");
        try {
            EntityUtils.deleteAll(service);
        } catch (ServiceFailureException ex) {
            LOGGER.error("Failed to clean database.", ex);
        }
    }

    private void createEntities() throws ServiceFailureException, URISyntaxException {
        for (int i = 0; i < 4; i++) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("string", generateString(i, 10));
            properties.put("boolean", i % 2 == 0);
            properties.put("int", i + 8);
            properties.put("intArray", generateIntArray(i + 8, 5));
            properties.put("intIntArray", generateIntIntArray(i + 8, 3));
            properties.put("objArray", generateObjectList(i + 8, 3));
            Thing thing = new Thing("Thing " + i, "It's a thing.");
            thing.setProperties(properties);
            service.create(thing);
            THINGS.add(thing);
        }

        Location location = new Location("Location 1", "Location of Thing 1.", "application/vnd.geo+json", new Point(8, 52));
        location.getThings().add(THINGS.get(0).withOnlyId());
        service.create(location);
        LOCATIONS.add(location);

        Sensor sensor = new Sensor("Sensor 1", "The first sensor.", "text", "Some metadata.");
        service.create(sensor);
        SENSORS.add(sensor);

        ObservedProperty obsProp = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
        service.create(obsProp);
        O_PROPS.add(obsProp);

        Datastream datastream = new Datastream("Datastream 1", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        datastream.setThing(THINGS.get(0));
        datastream.setSensor(sensor);
        datastream.setObservedProperty(obsProp);
        service.create(datastream);
        DATASTREAMS.add(datastream);

        for (int i = 0; i <= 12; i++) {
            Map<String, Object> parameters = new HashMap<>();
            Observation o = new Observation(i, datastream);
            parameters.put("string", generateString(i, 10));
            parameters.put("boolean", i % 2 == 0);
            parameters.put("int", i);
            parameters.put("intArray", generateIntArray(i, 5));
            parameters.put("intIntArray", generateIntIntArray(i, 3));
            parameters.put("objArray", generateObjectList(i, 3));
            o.setParameters(parameters);
            service.create(o);
            OBSERVATIONS.add(o);
        }
        {
            Map<String, Object> parameters = new HashMap<>();
            Observation o = new Observation("badVales1", datastream);
            parameters.put("int", generateString(13, 10));
            parameters.put("string", 13 % 2 == 0);
            parameters.put("boolean", 13);
            parameters.put("objArray", generateIntArray(13, 5));
            parameters.put("intArray", generateIntIntArray(13, 3));
            parameters.put("intIntArray", generateObjectList(13, 3));
            o.setParameters(parameters);
            service.create(o);
            OBSERVATIONS.add(o);
        }
        {
            Map<String, Object> parameters = new HashMap<>();
            Observation o = new Observation("badVales2", datastream);
            parameters.put("boolean", generateString(14, 10));
            parameters.put("int", 14 % 2 == 0);
            parameters.put("string", 14);
            parameters.put("intIntArray", generateIntArray(14, 5));
            parameters.put("objArray", generateIntIntArray(14, 3));
            parameters.put("intArray", generateObjectList(14, 3));
            o.setParameters(parameters);
            service.create(o);
            OBSERVATIONS.add(o);
        }
        {
            Map<String, Object> parameters = new HashMap<>();
            Observation o = new Observation("badVales3", datastream);
            parameters.put("boolean", "true");
            parameters.put("int", "5");
            o.setParameters(parameters);
            service.create(o);
            OBSERVATIONS.add(o);
        }
    }

    /**
     * Generates a string of letters, with the given length, starting at the
     * given letter, where a=0.
     *
     * @param startLetter the starting letter (a=0).
     * @param length The length of the string to generate.
     * @return The string.
     */
    public static String generateString(int startLetter, int length) {
        StringBuilder sb = new StringBuilder();
        char curLetter = (char) ('a' + startLetter % 26);
        for (int i = 0; i < length; i++) {
            sb.append(curLetter);
            curLetter++;
            if (curLetter > 'z') {
                curLetter = 'a';
            }
        }
        return sb.toString();
    }

    /**
     * Generates an array of numbers, with the given length, starting at the
     * given number.
     *
     * @param startValue the starting number.
     * @param length The length of the array to generate.
     * @return The string.
     */
    public static int[] generateIntArray(int startValue, int length) {
        int[] value = new int[length];
        int curVal = startValue;
        for (int i = 0; i < length; i++) {
            value[i] = curVal;
            curVal++;
        }
        return value;
    }

    public static int[][] generateIntIntArray(int startValue, int length) {
        int[][] value = new int[length][];
        int curVal = startValue;
        for (int i = 0; i < length; i++) {
            value[i] = generateIntArray(curVal, length);
            curVal++;
        }
        return value;
    }

    public static List<Object> generateObjectList(int startValue, int length) {
        List<Object> value = new ArrayList<>();
        int curVal = startValue;
        for (int i = 0; i < length; i++) {
            Map<String, Object> newObject = new HashMap<>();
            newObject.put("string", generateString(curVal, 10));
            newObject.put("boolean", curVal % 2 == 0);
            newObject.put("int", curVal);
            newObject.put("intArray", generateIntArray(curVal, 3));
            value.add(newObject);
            curVal++;
        }
        return value;
    }

    public void filterAndCheck(BaseDao doa, String filter, List<? extends Entity> expected) {
        try {
            EntityList<Observation> result = doa.query().filter(filter).list();
            EntityUtils.resultTestResult check = EntityUtils.resultContains(result, expected);
            Assert.assertTrue(check.testOk, "Failed on filter: " + filter + " Cause: " + check.message);
        } catch (ServiceFailureException ex) {
            Assert.fail("Failed to call service.", ex);
        }
    }

    @Test(description = "Low level tests", groups = "level-3", priority = 0)
    public void testFetchLowLevelThingProperties() throws ServiceFailureException {
        String urlString = rootUri + "/Things(" + THINGS.get(0).getId() + ")/properties/string";
        JsonNode json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "string", (String) THINGS.get(0).getProperties().get("string"), urlString);

        urlString = rootUri + "/Things(" + THINGS.get(0).getId() + ")/properties/boolean";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "boolean", (Boolean) THINGS.get(0).getProperties().get("boolean"), urlString);

        urlString = rootUri + "/Things(" + THINGS.get(0).getId() + ")/properties/int";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "int", (Integer) THINGS.get(0).getProperties().get("int"), urlString);

        urlString = rootUri + "/Things(" + THINGS.get(0).getId() + ")/properties/intArray";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "intArray", (int[]) THINGS.get(0).getProperties().get("intArray"), urlString);

        urlString = rootUri + "/Things(" + THINGS.get(0).getId() + ")/properties/intArray[1]";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "intArray[1]", ((int[]) THINGS.get(0).getProperties().get("intArray"))[1], urlString);

        urlString = rootUri + "/Things(" + THINGS.get(0).getId() + ")/properties/intIntArray[1]";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "intIntArray[1]", ((int[][]) THINGS.get(0).getProperties().get("intIntArray"))[1], urlString);

        urlString = rootUri + "/Things(" + THINGS.get(0).getId() + ")/properties/intIntArray[0][1]";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "intIntArray[0][1]", ((int[][]) THINGS.get(0).getProperties().get("intIntArray"))[0][1], urlString);

        urlString = rootUri + "/Things(" + THINGS.get(0).getId() + ")/properties/objArray[0]/string";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "string", ((List<Map<String, String>>) THINGS.get(0).getProperties().get("objArray")).get(0).get("string"), urlString);
    }

    @Test(description = "Low level tests", groups = "level-3", priority = 1)
    public void testFetchLowLevelObservationParameters() throws ServiceFailureException {
        String urlString = rootUri + "/Observations(" + OBSERVATIONS.get(0).getId() + ")/parameters/string";
        JsonNode json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "string", (String) OBSERVATIONS.get(0).getParameters().get("string"), urlString);

        urlString = rootUri + "/Observations(" + OBSERVATIONS.get(0).getId() + ")/parameters/boolean";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "boolean", (Boolean) OBSERVATIONS.get(0).getParameters().get("boolean"), urlString);

        urlString = rootUri + "/Observations(" + OBSERVATIONS.get(0).getId() + ")/parameters/int";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "int", (Integer) OBSERVATIONS.get(0).getParameters().get("int"), urlString);

        urlString = rootUri + "/Observations(" + OBSERVATIONS.get(0).getId() + ")/parameters/intArray";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "intArray", (int[]) OBSERVATIONS.get(0).getParameters().get("intArray"), urlString);

        urlString = rootUri + "/Observations(" + OBSERVATIONS.get(0).getId() + ")/parameters/intArray[1]";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "intArray[1]", ((int[]) OBSERVATIONS.get(0).getParameters().get("intArray"))[1], urlString);

        urlString = rootUri + "/Observations(" + OBSERVATIONS.get(0).getId() + ")/parameters/intIntArray[1]";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "intIntArray[1]", ((int[][]) OBSERVATIONS.get(0).getParameters().get("intIntArray"))[1], urlString);

        urlString = rootUri + "/Observations(" + OBSERVATIONS.get(0).getId() + ")/parameters/intIntArray[0][1]";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "intIntArray[0][1]", ((int[][]) OBSERVATIONS.get(0).getParameters().get("intIntArray"))[0][1], urlString);

        urlString = rootUri + "/Observations(" + OBSERVATIONS.get(0).getId() + ")/parameters/objArray[0]/string";
        json = getJsonObjectForResponse(urlString);
        testResponseProperty(json, "string", ((List<Map<String, String>>) OBSERVATIONS.get(0).getParameters().get("objArray")).get(0).get("string"), urlString);

    }

    @Test(description = "Test filter on string property in json", groups = "level-3", priority = 4)
    public void testStringFilter() throws ServiceFailureException {
        filterAndCheck(service.things(), "properties/string eq '" + THINGS.get(2).getProperties().get("string") + "'", getFromList(THINGS, 2));
        filterAndCheck(service.observations(), "parameters/string eq '" + OBSERVATIONS.get(2).getParameters().get("string") + "'", getFromList(OBSERVATIONS, 2));

        filterAndCheck(service.things(), "substringof('cdefgh', properties/string)", getFromList(THINGS, 0, 1, 2));
        filterAndCheck(service.observations(), "substringof('cdefgh', parameters/string)", getFromList(OBSERVATIONS, 0, 1, 2));

        filterAndCheck(service.things(), "properties/objArray[0]/string eq 'jklmnopqrs'", getFromList(THINGS, 1));
        filterAndCheck(service.observations(), "parameters/objArray[0]/string eq 'jklmnopqrs'", getFromList(OBSERVATIONS, 9));

        filterAndCheck(service.observations(), "parameters/int eq '5'", getFromList(OBSERVATIONS, 5, 15));
    }

    @Test(description = "Test filter on number property in json", groups = "level-3", priority = 4)
    public void testNumberFilter() throws ServiceFailureException {
        filterAndCheck(service.things(), "properties/int eq " + THINGS.get(2).getProperties().get("int"), getFromList(THINGS, 2));
        filterAndCheck(service.observations(), "parameters/int eq " + OBSERVATIONS.get(2).getParameters().get("int"), getFromList(OBSERVATIONS, 2));

        filterAndCheck(service.things(), "properties/int gt 9", getFromList(THINGS, 2, 3));
        filterAndCheck(service.observations(), "parameters/int gt 8", getFromList(OBSERVATIONS, 9, 10, 11, 12));

        filterAndCheck(service.things(), "properties/int lt 9", getFromList(THINGS, 0));
        filterAndCheck(service.observations(), "parameters/int lt 8", getFromList(OBSERVATIONS, 0, 1, 2, 3, 4, 5, 6, 7));

        filterAndCheck(service.things(), "properties/intArray[1] gt 10", getFromList(THINGS, 2, 3));
        filterAndCheck(service.observations(), "parameters/intArray[1] gt 9", getFromList(OBSERVATIONS, 9, 10, 11, 12));

        filterAndCheck(service.things(), "properties/intArray[1] lt 10", getFromList(THINGS, 0));
        filterAndCheck(service.observations(), "parameters/intArray[1] lt 9", getFromList(OBSERVATIONS, 0, 1, 2, 3, 4, 5, 6, 7));

        filterAndCheck(service.things(), "properties/intIntArray[1][0] gt 10", getFromList(THINGS, 2, 3));
        filterAndCheck(service.observations(), "parameters/intIntArray[1][0] gt 9", getFromList(OBSERVATIONS, 9, 10, 11, 12));

        filterAndCheck(service.things(), "properties/objArray[1]/intArray[0] gt 10", getFromList(THINGS, 2, 3));
        filterAndCheck(service.observations(), "parameters/objArray[1]/intArray[0] gt 9", getFromList(OBSERVATIONS, 9, 10, 11, 12));
    }

    @Test(description = "Test filter on boolean property in json", groups = "level-3", priority = 4)
    public void testBooleanFilter() throws ServiceFailureException {
        filterAndCheck(service.things(), "properties/boolean eq " + THINGS.get(1).getProperties().get("boolean"), getFromList(THINGS, 1, 3));
        filterAndCheck(service.observations(), "parameters/boolean eq " + OBSERVATIONS.get(1).getParameters().get("boolean"), getFromList(OBSERVATIONS, 1, 3, 5, 7, 9, 11));

        filterAndCheck(service.things(), "properties/boolean", getFromList(THINGS, 0, 2));
        filterAndCheck(service.observations(), "parameters/boolean", getFromList(OBSERVATIONS, 0, 2, 4, 6, 8, 10, 12));

        filterAndCheck(service.things(), "not properties/boolean", getFromList(THINGS, 1, 3));
        filterAndCheck(service.observations(), "not parameters/boolean", getFromList(OBSERVATIONS, 1, 3, 5, 7, 9, 11));

        filterAndCheck(service.things(), "properties/objArray[1]/boolean", getFromList(THINGS, 1, 3));
        filterAndCheck(service.observations(), "parameters/objArray[1]/boolean", getFromList(OBSERVATIONS, 1, 3, 5, 7, 9, 11));
    }

    private JsonNode getJsonObjectForResponse(String urlString) {
        Map<String, Object> responseMap = HTTPMethods.doGet(urlString);
        String response = responseMap.get("response").toString();
        int responseCode = Integer.parseInt(responseMap.get("response-code").toString());
        Assert.assertEquals(responseCode, 200, "Incorrect response code (" + responseCode + ") for url: " + urlString);
        JsonNode json;
        try {
            json = new ObjectMapper().readTree(response);
        } catch (IOException ex) {
            Assert.fail("Server returned malformed JSON for request: " + urlString, ex);
            return null;
        }

        if (!json.isObject()) {
            Assert.fail("Server did not return a JSON object for request: " + urlString);
        }
        return json;
    }

    private void testResponseProperty(JsonNode response, String propertyName, String expectedValue, String urlForError) {
        JsonNode value = response.get(propertyName);
        if (value == null || !value.isTextual()) {
            Assert.fail("field '" + propertyName + "' is not an string for request: " + urlForError);
            return;
        }
        Assert.assertEquals(value.textValue(), expectedValue, "field '" + propertyName + "' does not have the correct value for request: " + urlForError);
    }

    private void testResponseProperty(JsonNode response, String propertyName, Boolean expectedValue, String urlForError) {
        JsonNode value = response.get(propertyName);
        if (value == null || !value.isBoolean()) {
            Assert.fail("field '" + propertyName + "' is not an boolean for request: " + urlForError);
            return;
        }
        Assert.assertEquals(value.booleanValue(), expectedValue.booleanValue(), "field '" + propertyName + "' does not have the correct value for request: " + urlForError);
    }

    private void testResponseProperty(JsonNode response, String propertyName, Integer expectedValue, String urlForError) {
        JsonNode value = response.get(propertyName);
        if (value == null || !value.isInt()) {
            Assert.fail("field '" + propertyName + "' is not an integer for request: " + urlForError);
            return;
        }
        Assert.assertEquals(value.intValue(), expectedValue.intValue(), "field '" + propertyName + "' does not have the correct value for request: " + urlForError);
    }

    private void testResponseProperty(JsonNode response, String propertyName, int[] expectedValue, String urlForError) {
        JsonNode value = response.get(propertyName);
        if (value == null || !value.isArray()) {
            Assert.fail("field '" + propertyName + "' is not an array for request: " + urlForError);
            return;
        }
        Assert.assertEquals(value.size(), expectedValue.length, "array '" + propertyName + "' does not have the correct size for request: " + urlForError);

        int i = 0;
        for (Iterator<JsonNode> it = value.elements(); it.hasNext();) {
            JsonNode element = it.next();
            if (!element.isInt()) {
                Assert.fail("array '" + propertyName + "' contains non-integer element '" + element.toString() + "' for request: " + urlForError);
            }
            Assert.assertEquals(element.intValue(), expectedValue[i], "array '" + propertyName + "' contains incorrect value at position " + i + " for request: " + urlForError);
            i++;
        }
    }

    public static <T extends Entity<T>> List<T> getFromList(List<T> list, int... ids) {
        List<T> result = new ArrayList<>();
        for (int i : ids) {
            result.add(list.get(i));
        }
        return result;
    }
}

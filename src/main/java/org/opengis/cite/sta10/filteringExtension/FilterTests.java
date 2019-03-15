package org.opengis.cite.sta10.filteringExtension;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.dao.ObservedPropertyDao;
import de.fraunhofer.iosb.ilt.sta.dao.ThingDao;
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
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.geojson.LineString;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.geojson.Polygon;
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
import org.threeten.extra.Interval;

/**
 * Some odd tests.
 *
 * @author Hylke van der Schaaf
 */
public class FilterTests {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(FilterTests.class);
    private String rootUri;
    private SensorThingsService service;
    private final List<Thing> THINGS = new ArrayList<>();
    private final List<Location> LOCATIONS = new ArrayList<>();
    private final List<Sensor> SENSORS = new ArrayList<>();
    private final List<ObservedProperty> O_PROPS = new ArrayList<>();
    private final List<Datastream> DATASTREAMS = new ArrayList<>();
    private final List<Observation> OBSERVATIONS = new ArrayList<>();

    public FilterTests() {
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
        Thing thing = new Thing("Thing 0", "The first thing.");
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 1", "The second thing.");
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 2", "The third thing.");
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 3", "The fourth thing.");
        service.create(thing);
        THINGS.add(thing);

        // Locations 0
        Location location = new Location("Location 1.0", "First Location of Thing 1.", "application/vnd.geo+json", new Point(8, 51));
        location.getThings().add(THINGS.get(0));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 1
        location = new Location("Location 1.1", "Second Location of Thing 1.", "application/vnd.geo+json", new Point(8, 52));
        location.getThings().add(THINGS.get(0));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 2
        location = new Location("Location 2", "Location of Thing 2.", "application/vnd.geo+json", new Point(8, 53));
        location.getThings().add(THINGS.get(1));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 3
        location = new Location("Location 3", "Location of Thing 3.", "application/vnd.geo+json", new Point(8, 54));
        location.getThings().add(THINGS.get(2));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 4
        location = new Location("Location 4", "Location of Thing 4.", "application/vnd.geo+json",
                new Polygon(
                        new LngLatAlt(8, 53),
                        new LngLatAlt(7, 52),
                        new LngLatAlt(7, 53),
                        new LngLatAlt(8, 53)));
        location.getThings().add(THINGS.get(3));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 5
        location = new Location("Location 5", "A line.", "application/vnd.geo+json",
                new LineString(
                        new LngLatAlt(5, 52),
                        new LngLatAlt(5, 53)));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 6
        location = new Location("Location 6", "A longer line.", "application/vnd.geo+json",
                new LineString(
                        new LngLatAlt(5, 52),
                        new LngLatAlt(6, 53)));
        service.create(location);
        LOCATIONS.add(location);

        // Locations 7
        location = new Location("Location 7", "The longest line.", "application/vnd.geo+json",
                new LineString(
                        new LngLatAlt(4, 52),
                        new LngLatAlt(8, 52)));
        service.create(location);
        LOCATIONS.add(location);

        createSensor("Sensor 0", "The sensor with idx 0.", "text", "Some metadata.");
        createSensor("Sensor 1", "The sensor with idx 0.", "text", "Some metadata.");
        createSensor("Sensor 2", "The sensor with idx 0.", "text", "Some metadata.");
        createSensor("Sensor 3", "The sensor with idx 0.", "text", "Some metadata.");

        createObservedProperty("ObservedProperty 0", new URI("http://ucom.org/temperature"), "ObservedProperty with index 0.");
        createObservedProperty("ObservedProperty 1", new URI("http://ucom.org/humidity"), "ObservedProperty with index 1.");
        createObservedProperty("ObservedProperty 2", new URI("http://ucom.org/pressure"), "ObservedProperty with index 2.");
        createObservedProperty("ObservedProperty 3", new URI("http://ucom.org/turbidity"), "ObservedProperty with index 3.");

        UnitOfMeasurement uomTemp = new UnitOfMeasurement("degree celcius", "°C", "ucum:T");
        UnitOfMeasurement uomHumi = new UnitOfMeasurement("percent", "%", "ucum:%");
        UnitOfMeasurement uomPres = new UnitOfMeasurement("milibar", "mbar", "ucum:mbar");
        UnitOfMeasurement uomTurb = new UnitOfMeasurement("-", "-", "-");

        createDatastream("Datastream 0", "Datastream 1 of thing 0, sensor 0.", "someType", uomTemp, THINGS.get(0), SENSORS.get(0), O_PROPS.get(0));
        createDatastream("Datastream 1", "Datastream 2 of thing 0, sensor 1.", "someType", uomTemp, THINGS.get(0), SENSORS.get(1), O_PROPS.get(1));
        createDatastream("Datastream 2", "Datastream 3 of thing 0, sensor 2.", "someType", uomTemp, THINGS.get(0), SENSORS.get(2), O_PROPS.get(2));
        createDatastream("Datastream 3", "Datastream 1 of thing 1, sensor 0.", "someType", uomTemp, THINGS.get(1), SENSORS.get(0), O_PROPS.get(0));
        createDatastream("Datastream 4", "Datastream 2 of thing 1, sensor 1.", "someType", uomTemp, THINGS.get(1), SENSORS.get(1), O_PROPS.get(1));
        createDatastream("Datastream 5", "Datastream 3 of thing 1, sensor 3.", "someType", uomTemp, THINGS.get(1), SENSORS.get(3), O_PROPS.get(3));

        ZonedDateTime startTime = ZonedDateTime.parse("2016-01-01T01:00:00.000Z");
        Interval startInterval = Interval.of(Instant.parse("2016-01-01T01:00:00.000Z"), Instant.parse("2016-01-01T02:00:00.000Z"));

        createObservationSet(DATASTREAMS.get(0), 0, startTime, startInterval, 6);
        createObservationSet(DATASTREAMS.get(1), 3, startTime, startInterval, 6);
        createObservationSet(DATASTREAMS.get(2), 6, startTime, startInterval, 6);
        createObservationSet(DATASTREAMS.get(3), 9, startTime, startInterval, 6);
        createObservationSet(DATASTREAMS.get(4), 12, startTime, startInterval, 6);
        createObservationSet(DATASTREAMS.get(5), 15, startTime, startInterval, 6);

    }

    private Sensor createSensor(String name, String desc, String type, String metadata) throws ServiceFailureException {
        int idx = SENSORS.size();
        Map<String, Object> properties = new HashMap<>();
        properties.put("idx", idx);

        Sensor sensor = new Sensor(name, desc, type, metadata);
        sensor.setProperties(properties);
        service.create(sensor);
        SENSORS.add(sensor);
        return sensor;
    }

    private Datastream createDatastream(String name, String desc, String type, UnitOfMeasurement uom, Thing thing, Sensor sensor, ObservedProperty op) throws ServiceFailureException {
        int idx = DATASTREAMS.size();
        Map<String, Object> properties = new HashMap<>();
        properties.put("idx", idx);

        Datastream ds = new Datastream(name, desc, type, uom);
        ds.setProperties(properties);
        ds.setThing(thing);
        ds.setSensor(sensor);
        ds.setObservedProperty(op);
        service.create(ds);
        DATASTREAMS.add(ds);
        return ds;
    }

    private ObservedProperty createObservedProperty(String name, URI definition, String description) throws ServiceFailureException {
        int idx = O_PROPS.size();
        Map<String, Object> properties = new HashMap<>();
        properties.put("idx", idx);
        ObservedProperty obsProp = new ObservedProperty(name, definition, description);
        obsProp.setProperties(properties);
        service.create(obsProp);
        O_PROPS.add(obsProp);
        return obsProp;
    }

    private void createObservationSet(Datastream datastream, long resultStart, ZonedDateTime phenomenonTimeStart, Interval validTimeStart, int count) throws ServiceFailureException {
        for (int i = 0; i < count; i++) {
            ZonedDateTime phenTime = phenomenonTimeStart.plus(i, ChronoUnit.HOURS);
            Interval validTime = Interval.of(
                    validTimeStart.getStart().plus(count, ChronoUnit.HOURS),
                    validTimeStart.getEnd().plus(count, ChronoUnit.HOURS));
            createObservation(datastream, resultStart + i, phenTime, validTime);
        }
    }

    private Observation createObservation(Datastream datastream, long result, ZonedDateTime phenomenonTime, Interval validTime) throws ServiceFailureException {
        int idx = OBSERVATIONS.size();
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("idx", idx);
        Observation obs = new Observation(result, datastream);
        obs.setPhenomenonTimeFrom(phenomenonTime);
        obs.setValidTime(validTime);
        obs.setParameters(parameters);
        service.create(obs);
        OBSERVATIONS.add(obs);
        return obs;
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

    @Test(description = "Test indirect filters.", groups = "level-3")
    public void testIndirectFilter() throws ServiceFailureException {
        ThingDao doa = service.things();
        filterAndCheck(doa, "Locations/name eq 'Location 2'", getFromList(THINGS, 1));
        filterAndCheck(doa, "startswith(HistoricalLocations/Location/name, 'Location 1')", getFromList(THINGS, 0));
    }

    @Test(description = "Test deep indirect filters.", groups = "level-3")
    public void testDeepIndirection() throws ServiceFailureException {
        ObservedPropertyDao doa = service.observedProperties();

        filterAndCheck(doa, "Datastream/Thing/Datastreams/ObservedProperty/name eq 'ObservedProperty 0'", getFromList(O_PROPS, 0, 1, 2, 3));
        filterAndCheck(doa, "Datastream/Thing/Datastreams/ObservedProperty/name eq 'ObservedProperty 3'", getFromList(O_PROPS, 0, 1, 3));
    }

    @Test(description = "Test combined deep indirect filters.", groups = "level-3")
    public void testParallelIndirection() {
        filterAndCheck(service.things(), "Datastreams/ObservedProperties/name eq 'ObservedProperty 1' and Datastreams/Observations/result gt 10", getFromList(THINGS, 1));
    }

    @Test(description = "If the property is single-valued and has the null value, the service SHALL respond with 204 No Content.", groups = "level-3")
    public void testNullEntityProperty() {
        String requestUrl = rootUri + "/Things(" + THINGS.get(0).getId().getUrl() + ")/properties";
        Map<String, Object> result = HTTPMethods.doGet(requestUrl);
        int responseCode = Integer.parseInt(result.get("response-code").toString());
        if (responseCode != 204) {
            Assert.fail("Expected response code 204 on request " + requestUrl);
        }
    }

    @Test(description = "If the property is single-valued and has the null value, the service SHALL respond with 204 No Content.", groups = "level-3")
    public void testNullEntityPropertyValue() {
        String requestUrl = rootUri + "/Things(" + THINGS.get(0).getId().getUrl() + ")/properties/$value";
        Map<String, Object> result = HTTPMethods.doGet(requestUrl);
        int responseCode = Integer.parseInt(result.get("response-code").toString());
        if (responseCode != 204) {
            Assert.fail("Expected response code 204 on request " + requestUrl);
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

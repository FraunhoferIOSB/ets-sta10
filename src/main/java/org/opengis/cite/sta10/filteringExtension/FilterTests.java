package org.opengis.cite.sta10.filteringExtension;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.dao.ObservationDao;
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
import java.util.ArrayList;
import java.util.List;
import org.geojson.LineString;
import org.geojson.LngLatAlt;
import org.geojson.Point;
import org.geojson.Polygon;
import org.opengis.cite.sta10.SuiteAttribute;
import org.opengis.cite.sta10.util.EntityUtils;
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
        Thing thing = new Thing("Thing 1", "The first thing.");
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 2", "The second thing.");
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 3", "The third thing.");
        service.create(thing);
        THINGS.add(thing);

        thing = new Thing("Thing 4", "The fourt thing.");
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

        Sensor sensor = new Sensor("Sensor 1", "The first sensor.", "text", "Some metadata.");
        service.create(sensor);
        SENSORS.add(sensor);

        ObservedProperty obsProp = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
        service.create(obsProp);
        O_PROPS.add(obsProp);

        Datastream datastream = new Datastream("Datastream 1", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        datastream.setThing(thing);
        datastream.setSensor(sensor);
        datastream.setObservedProperty(obsProp);
        service.create(datastream);
        DATASTREAMS.add(datastream);

        Observation o = new Observation(1, datastream);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-01T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-01T01:01:01.000Z"), Instant.parse("2016-01-01T23:59:59.999Z")));
        service.create(o);
        OBSERVATIONS.add(o);

        o = new Observation(2, datastream);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-02T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-02T01:01:01.000Z"), Instant.parse("2016-01-02T23:59:59.999Z")));
        service.create(o);
        OBSERVATIONS.add(o);

        o = new Observation(3, datastream);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-03T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-03T01:01:01.000Z"), Instant.parse("2016-01-03T23:59:59.999Z")));
        service.create(o);
        OBSERVATIONS.add(o);

        o = new Observation(4, datastream);
        o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-04T01:01:01.000Z"));
        o.setValidTime(Interval.of(Instant.parse("2016-01-04T01:01:01.000Z"), Instant.parse("2016-01-04T23:59:59.999Z")));
        service.create(o);
        OBSERVATIONS.add(o);

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

    @Test(description = "Test different time interval and duration filters.", groups = "level-3")
    public void testTimeInterval() throws ServiceFailureException {
        ObservationDao doa = service.observations();

        filterAndCheck(doa, "not validTime lt 2016-01-03T12:00:00Z and not validTime gt 2016-01-03T12:00:00Z", getFromList(OBSERVATIONS, 2));

        // Durations
        filterAndCheck(doa, "validTime add duration'P1D' gt 2016-01-03T01:01:00Z", getFromList(OBSERVATIONS, 1, 2, 3));
        filterAndCheck(doa, "validTime gt 2016-01-03T01:01:00Z sub duration'P1D'", getFromList(OBSERVATIONS, 1, 2, 3));
        filterAndCheck(doa, "validTime sub duration'P1D' gt 2016-01-03T01:01:00Z", getFromList(OBSERVATIONS, 3));

        filterAndCheck(doa, "validTime lt 2016-01-02T01:01:01.000Z/2016-01-03T23:59:59.999Z add duration'P1D'", getFromList(OBSERVATIONS, 0, 1));
        filterAndCheck(doa, "validTime gt 2016-01-02T01:01:01.000Z/2016-01-03T23:59:59.999Z sub duration'P1D'", getFromList(OBSERVATIONS, 2, 3));

        filterAndCheck(doa, "phenomenonTime sub 2016-01-03T01:01:01.000Z eq duration'P1D'", getFromList(OBSERVATIONS, 3));
    }

    public static <T extends Entity<T>> List<T> getFromList(List<T> list, int... ids) {
        List<T> result = new ArrayList<>();
        for (int i : ids) {
            result.add(list.get(i));
        }
        return result;
    }
}

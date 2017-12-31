package org.opengis.cite.sta10.filteringExtension;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.FeatureOfInterest;
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
 * Tests for the geospatial functions.
 *
 * @author Hylke van der Schaaf
 */
public class GeoTests {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(GeoTests.class);
    private String rootUri;
    private SensorThingsService service;
    private final List<Datastream> DATASTREAMS = new ArrayList<>();
    private final List<FeatureOfInterest> FEATURESOFINTEREST = new ArrayList<>();
    private final List<Location> LOCATIONS = new ArrayList<>();
    private final List<Observation> OBSERVATIONS = new ArrayList<>();
    private final List<ObservedProperty> O_PROPS = new ArrayList<>();
    private final List<Sensor> SENSORS = new ArrayList<>();
    private final List<Thing> THINGS = new ArrayList<>();

    public GeoTests() {
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
        {
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
        }
        {
            Sensor sensor = new Sensor("Sensor 1", "The first sensor.", "text", "Some metadata.");
            service.create(sensor);
            SENSORS.add(sensor);
        }
        {
            ObservedProperty obsProp = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
            service.create(obsProp);
            O_PROPS.add(obsProp);
        }
        {
            Datastream datastream = new Datastream("Datastream 1", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
            datastream.setThing(THINGS.get(0));
            datastream.setSensor(SENSORS.get(0));
            datastream.setObservedProperty(O_PROPS.get(0));
            service.create(datastream);
            DATASTREAMS.add(datastream);

            datastream = new Datastream("Datastream 2", "The temperature of thing 2, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
            datastream.setThing(THINGS.get(1));
            datastream.setSensor(SENSORS.get(0));
            datastream.setObservedProperty(O_PROPS.get(0));
            service.create(datastream);
            DATASTREAMS.add(datastream);

            datastream = new Datastream("Datastream 3", "The temperature of thing 3, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
            datastream.setThing(THINGS.get(2));
            datastream.setSensor(SENSORS.get(0));
            datastream.setObservedProperty(O_PROPS.get(0));
            service.create(datastream);
            DATASTREAMS.add(datastream);
        }
        {
            // Locations 0
            Point gjo = new Point(8, 51);
            Location location = new Location("Location 1.0", "First Location of Thing 1.", "application/vnd.geo+json", gjo);
            location.getThings().add(THINGS.get(0));
            service.create(location);
            LOCATIONS.add(location);

            FeatureOfInterest featureOfInterest = new FeatureOfInterest("FoI 0", "This should be FoI #0.", "application/geo+json", gjo);
            service.create(featureOfInterest);
            FEATURESOFINTEREST.add(featureOfInterest);

            Observation o = new Observation(1, DATASTREAMS.get(0));
            o.setFeatureOfInterest(featureOfInterest);
            o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-01T01:01:01.000Z"));
            o.setValidTime(Interval.of(Instant.parse("2016-01-01T01:01:01.000Z"), Instant.parse("2016-01-01T23:59:59.999Z")));
            service.create(o);
            OBSERVATIONS.add(o);
        }
        {
            // Locations 1
            Point gjo = new Point(8, 52);
            Location location = new Location("Location 1.1", "Second Location of Thing 1.", "application/vnd.geo+json", gjo);
            location.getThings().add(THINGS.get(0));
            service.create(location);
            LOCATIONS.add(location);

            FeatureOfInterest featureOfInterest = new FeatureOfInterest("FoI 1", "This should be FoI #1.", "application/geo+json", gjo);
            service.create(featureOfInterest);
            FEATURESOFINTEREST.add(featureOfInterest);

            Observation o = new Observation(2, DATASTREAMS.get(0));
            o.setFeatureOfInterest(featureOfInterest);
            o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-02T01:01:01.000Z"));
            o.setValidTime(Interval.of(Instant.parse("2016-01-02T01:01:01.000Z"), Instant.parse("2016-01-02T23:59:59.999Z")));
            service.create(o);
            OBSERVATIONS.add(o);
        }
        {
            // Locations 2
            Point gjo = new Point(8, 53);
            Location location = new Location("Location 2", "Location of Thing 2.", "application/vnd.geo+json", gjo);
            location.getThings().add(THINGS.get(1));
            service.create(location);
            LOCATIONS.add(location);

            FeatureOfInterest featureOfInterest = new FeatureOfInterest("FoI 2", "This should be FoI #2.", "application/geo+json", gjo);
            service.create(featureOfInterest);
            FEATURESOFINTEREST.add(featureOfInterest);

            Observation o = new Observation(3, DATASTREAMS.get(1));
            o.setFeatureOfInterest(featureOfInterest);
            o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-03T01:01:01.000Z"));
            o.setValidTime(Interval.of(Instant.parse("2016-01-03T01:01:01.000Z"), Instant.parse("2016-01-03T23:59:59.999Z")));
            service.create(o);
            OBSERVATIONS.add(o);
        }
        {
            // Locations 3
            Point gjo = new Point(8, 54);
            Location location = new Location("Location 3", "Location of Thing 3.", "application/vnd.geo+json", gjo);
            location.getThings().add(THINGS.get(2));
            service.create(location);
            LOCATIONS.add(location);

            FeatureOfInterest featureOfInterest = new FeatureOfInterest("FoI 3", "This should be FoI #3.", "application/geo+json", gjo);
            service.create(featureOfInterest);
            FEATURESOFINTEREST.add(featureOfInterest);

            Observation o = new Observation(4, DATASTREAMS.get(2));
            o.setFeatureOfInterest(featureOfInterest);
            o.setPhenomenonTimeFrom(ZonedDateTime.parse("2016-01-04T01:01:01.000Z"));
            o.setValidTime(Interval.of(Instant.parse("2016-01-04T01:01:01.000Z"), Instant.parse("2016-01-04T23:59:59.999Z")));
            service.create(o);
            OBSERVATIONS.add(o);
        }
        {
            // Locations 4
            Polygon gjo = new Polygon(
                    new LngLatAlt(8, 53),
                    new LngLatAlt(7, 52),
                    new LngLatAlt(7, 53),
                    new LngLatAlt(8, 53));
            Location location = new Location("Location 4", "Location of Thing 4.", "application/vnd.geo+json", gjo);
            location.getThings().add(THINGS.get(3));
            service.create(location);
            LOCATIONS.add(location);

            FeatureOfInterest featureOfInterest = new FeatureOfInterest("FoI 4", "This should be FoI #4.", "application/geo+json", gjo);
            service.create(featureOfInterest);
            FEATURESOFINTEREST.add(featureOfInterest);
        }
        {
            // Locations 5
            LineString gjo = new LineString(
                    new LngLatAlt(5, 52),
                    new LngLatAlt(5, 53));
            Location location = new Location("Location 5", "A line.", "application/vnd.geo+json", gjo);
            service.create(location);
            LOCATIONS.add(location);

            FeatureOfInterest featureOfInterest = new FeatureOfInterest("FoI 5", "This should be FoI #5.", "application/geo+json", gjo);
            service.create(featureOfInterest);
            FEATURESOFINTEREST.add(featureOfInterest);
        }
        {
            // Locations 6
            LineString gjo = new LineString(
                    new LngLatAlt(5, 52),
                    new LngLatAlt(6, 53));
            Location location = new Location("Location 6", "A longer line.", "application/vnd.geo+json", gjo);
            service.create(location);
            LOCATIONS.add(location);

            FeatureOfInterest featureOfInterest = new FeatureOfInterest("FoI 6", "This should be FoI #6.", "application/geo+json", gjo);
            service.create(featureOfInterest);
            FEATURESOFINTEREST.add(featureOfInterest);
        }
        {
            // Locations 7
            LineString gjo = new LineString(
                    new LngLatAlt(4, 52),
                    new LngLatAlt(8, 52));
            Location location = new Location("Location 7", "The longest line.", "application/vnd.geo+json",
                    gjo);
            service.create(location);
            LOCATIONS.add(location);

            FeatureOfInterest featureOfInterest = new FeatureOfInterest("FoI 7", "This should be FoI #7.", "application/geo+json", gjo);
            service.create(featureOfInterest);
            FEATURESOFINTEREST.add(featureOfInterest);
        }
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

    @Test(description = "Test filter function geo.distance", groups = "level-3")
    public void testGeoDistance() throws ServiceFailureException {
        filterAndCheck(service.locations(), "geo.distance(location, geography'POINT(8 54.1)') lt 1", getFromList(LOCATIONS, 3));
        filterAndCheck(service.locations(), "geo.distance(location, geography'POINT(8 54.1)') gt 1", getFromList(LOCATIONS, 0, 1, 2, 4, 5, 6, 7));
        filterAndCheck(service.observations(), "geo.distance(FeatureOfInterest/feature, geography'POINT(8 54.1)') lt 1", getFromList(OBSERVATIONS, 3));
        filterAndCheck(service.observations(), "geo.distance(FeatureOfInterest/feature, geography'POINT(8 54.1)') gt 1", getFromList(OBSERVATIONS, 0, 1, 2));
    }

    @Test(description = "Test filter function geo.intersects", groups = "level-3")
    public void testGeoIntersects() throws ServiceFailureException {
        filterAndCheck(service.locations(), "geo.intersects(location, geography'LINESTRING(7.5 51, 7.5 54)')", getFromList(LOCATIONS, 4, 7));
        filterAndCheck(service.featuresOfInterest(), "geo.intersects(feature, geography'LINESTRING(7.5 51, 7.5 54)')", getFromList(FEATURESOFINTEREST, 4, 7));
    }

    @Test(description = "Test filter function geo.length", groups = "level-3")
    public void testGeoLength() throws ServiceFailureException {
        filterAndCheck(service.locations(), "geo.length(location) gt 1", getFromList(LOCATIONS, 6, 7));
        filterAndCheck(service.locations(), "geo.length(location) ge 1", getFromList(LOCATIONS, 5, 6, 7));
        filterAndCheck(service.locations(), "geo.length(location) eq 1", getFromList(LOCATIONS, 5));
        filterAndCheck(service.locations(), "geo.length(location) ne 1", getFromList(LOCATIONS, 0, 1, 2, 3, 4, 6, 7));
        filterAndCheck(service.locations(), "geo.length(location) le 4", getFromList(LOCATIONS, 0, 1, 2, 3, 4, 5, 6, 7));
        filterAndCheck(service.locations(), "geo.length(location) lt 4", getFromList(LOCATIONS, 0, 1, 2, 3, 4, 5, 6));
        filterAndCheck(service.featuresOfInterest(), "geo.length(feature) gt 1", getFromList(FEATURESOFINTEREST, 6, 7));
        filterAndCheck(service.featuresOfInterest(), "geo.length(feature) ge 1", getFromList(FEATURESOFINTEREST, 5, 6, 7));
        filterAndCheck(service.featuresOfInterest(), "geo.length(feature) eq 1", getFromList(FEATURESOFINTEREST, 5));
        filterAndCheck(service.featuresOfInterest(), "geo.length(feature) ne 1", getFromList(FEATURESOFINTEREST, 0, 1, 2, 3, 4, 6, 7));
        filterAndCheck(service.featuresOfInterest(), "geo.length(feature) le 4", getFromList(FEATURESOFINTEREST, 0, 1, 2, 3, 4, 5, 6, 7));
        filterAndCheck(service.featuresOfInterest(), "geo.length(feature) lt 4", getFromList(FEATURESOFINTEREST, 0, 1, 2, 3, 4, 5, 6));
    }

    @Test(description = "Test filter function st_contains", groups = "level-3")
    public void testStContains() throws ServiceFailureException {
        filterAndCheck(service.locations(),
                "st_contains(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', location)",
                getFromList(LOCATIONS, 1, 2));
        filterAndCheck(service.observations(),
                "st_contains(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', FeatureOfInterest/feature)",
                getFromList(OBSERVATIONS, 1, 2));
    }

    @Test(description = "Test filter function st_crosses", groups = "level-3")
    public void testStCrosses() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_crosses(geography'LINESTRING(7.5 51.5, 7.5 53.5)', location)", getFromList(LOCATIONS, 4, 7));
        filterAndCheck(service.featuresOfInterest(), "st_crosses(geography'LINESTRING(7.5 51.5, 7.5 53.5)', feature)", getFromList(FEATURESOFINTEREST, 4, 7));
    }

    @Test(description = "Test filter function st_disjoint", groups = "level-3")
    public void testStDisjoint() throws ServiceFailureException {
        filterAndCheck(service.locations(),
                "st_disjoint(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', location)",
                getFromList(LOCATIONS, 0, 3, 5, 6));
        filterAndCheck(service.featuresOfInterest(),
                "st_disjoint(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', feature)",
                getFromList(FEATURESOFINTEREST, 0, 3, 5, 6));
    }

    @Test(description = "Test filter function st_equals", groups = "level-3")
    public void testStEquals() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_equals(location, geography'POINT(8 53)')", getFromList(LOCATIONS, 2));
        filterAndCheck(service.featuresOfInterest(), "st_equals(feature, geography'POINT(8 53)')", getFromList(FEATURESOFINTEREST, 2));
    }

    @Test(description = "Test filter function st_intersects", groups = "level-3")
    public void testStIntersects() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_intersects(location, geography'LINESTRING(7.5 51, 7.5 54)')", getFromList(LOCATIONS, 4, 7));
        filterAndCheck(service.featuresOfInterest(), "st_intersects(feature, geography'LINESTRING(7.5 51, 7.5 54)')", getFromList(FEATURESOFINTEREST, 4, 7));
    }

    @Test(description = "Test filter function st_overlaps", groups = "level-3")
    public void testStOverlaps() throws ServiceFailureException {
        filterAndCheck(service.locations(),
                "st_overlaps(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', location)",
                getFromList(LOCATIONS, 4));
        filterAndCheck(service.featuresOfInterest(),
                "st_overlaps(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', feature)",
                getFromList(FEATURESOFINTEREST, 4));
    }

    @Test(description = "Test filter function st_relate", groups = "level-3")
    public void testStRelate() throws ServiceFailureException {
        filterAndCheck(service.locations(),
                "st_relate(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', location, 'T********')",
                getFromList(LOCATIONS, 1, 2, 4, 7));
        filterAndCheck(service.featuresOfInterest(),
                "st_relate(geography'POLYGON((7.5 51.5, 7.5 53.5, 8.5 53.5, 8.5 51.5, 7.5 51.5))', feature, 'T********')",
                getFromList(FEATURESOFINTEREST, 1, 2, 4, 7));
    }

    @Test(description = "Test filter function st_touches", groups = "level-3")
    public void testStTouches() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_touches(geography'POLYGON((8 53, 7.5 54.5, 8.5 54.5, 8 53))', location)", getFromList(LOCATIONS, 2, 4));
        filterAndCheck(service.featuresOfInterest(), "st_touches(geography'POLYGON((8 53, 7.5 54.5, 8.5 54.5, 8 53))', feature)", getFromList(FEATURESOFINTEREST, 2, 4));
    }

    @Test(description = "Test filter function st_within", groups = "level-3")
    public void testStWithin() throws ServiceFailureException {
        filterAndCheck(service.locations(), "st_within(geography'POINT(7.5 52.75)', location)", getFromList(LOCATIONS, 4));
        filterAndCheck(service.featuresOfInterest(), "st_within(geography'POINT(7.5 52.75)', feature)", getFromList(FEATURESOFINTEREST, 4));
    }

    public static <T extends Entity<T>> List<T> getFromList(List<T> list, int... ids) {
        List<T> result = new ArrayList<>();
        for (int i : ids) {
            result.add(list.get(i));
        }
        return result;
    }
}

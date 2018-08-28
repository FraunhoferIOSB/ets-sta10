package org.opengis.cite.sta10.createUpdateDelete;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.ObservationDao;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.FeatureOfInterest;
import de.fraunhofer.iosb.ilt.sta.model.HistoricalLocation;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.builder.HistoricalLocationBuilder;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import org.geojson.Point;
import org.opengis.cite.sta10.SuiteAttribute;
import org.opengis.cite.sta10.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests additional details not part of the official tests.
 *
 * @author Hylke van der Schaaf
 */
public class AdditionalTests {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(AdditionalTests.class);
    private String rootUri;
    private SensorThingsService service;
    private final List<Thing> things = new ArrayList<>();
    private final List<Datastream> datastreams = new ArrayList<>();
    private final List<Observation> observations = new ArrayList<>();

    public AdditionalTests() {
    }

    @BeforeClass()
    public void setUp(ITestContext testContext) {
        LOGGER.info("Setting up class.");
        Object obj = testContext.getSuite().getAttribute(
                SuiteAttribute.LEVEL.getName());
        if ((null != obj)) {
            Integer level = Integer.class.cast(obj);
            Assert.assertTrue(level.intValue() > 1,
                    "Conformance level 2 will not be checked since ics = " + level);
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
        } catch (MalformedURLException ex) {
            LOGGER.error("Failed to create service uri.", ex);
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

    @Test(description = "Test boolean result values.", groups = "level-2")
    public void testMultipleLocations() throws ServiceFailureException, URISyntaxException {
        EntityUtils.deleteAll(service);

        Thing thing = new Thing("Thing 1", "The first thing.");

        Location location1 = new Location("Location 1.0, Address", "The address of Thing 1.", "text/plain", "");
        thing.getLocations().add(location1);
        Location location2 = new Location("Location 1.0", "Location of Thing 1.", "application/vnd.geo+json", new Point(8, 51));
        thing.getLocations().add(location2);
        Location location3 = new Location("Location 1.0, Directions", "How to find Thing 1 in human language.", "text/plain", "");
        thing.getLocations().add(location3);

        service.create(thing);
        things.add(thing);

        Sensor sensor = new Sensor("Sensor 1", "The first sensor.", "text", "Some metadata.");
        ObservedProperty obsProp = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
        Datastream datastream = new Datastream("Datastream 1", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        datastream.setSensor(sensor);
        datastream.setObservedProperty(obsProp);
        datastream.setThing(thing);

        service.create(datastream);
        datastreams.add(datastream);

        ObservationDao doa = service.observations();
        Observation observation = new Observation(1.0, datastreams.get(0));
        doa.create(observation);
        observations.add(observation);

        Observation found;
        found = doa.find(observation.getId());
        FeatureOfInterest featureOfInterest = found.getFeatureOfInterest();
    }

    @Test
    public void testHistoricalLocationThing() throws ServiceFailureException {
        EntityUtils.deleteAll(service);

        // Create a thing
        Thing thing = new Thing("Thing 1", "The first thing.");
        service.create(thing);

        // Create three locations.
        Location location1 = new Location("Location 1.0", "Location Number 1.", "application/vnd.geo+json", new Point(8, 50));
        Location location2 = new Location("Location 2.0", "Location Number 2.", "application/vnd.geo+json", new Point(8, 51));
        Location location3 = new Location("Location 3.0", "Location Number 3.", "application/vnd.geo+json", new Point(8, 52));
        service.create(location1);
        service.create(location2);
        service.create(location3);

        // Give the Thing location 1
        thing.getLocations().add(location1.withOnlyId());
        service.update(thing);

        // Get the generated HistoricalLocation and change the time to a known value.
        List<HistoricalLocation> histLocations = thing.historicalLocations().query().list().toList();
        Assert.assertEquals(histLocations.size(), 1, "Incorrect number of HistoricalLocations for Thing.");
        HistoricalLocation histLocation = histLocations.get(0);
        histLocation.setTime(ZonedDateTime.parse("2016-01-01T06:00:00.000Z"));
        service.update(histLocation);

        // Now create a new HistoricalLocation for the Thing, with a later time.
        HistoricalLocation histLocation2 = HistoricalLocationBuilder.builder()
                .location(location2)
                .time(ZonedDateTime.parse("2016-01-01T07:00:00.000Z"))
                .thing(thing.withOnlyId())
                .build();
        service.create(histLocation2);

        // Check if the Location of the Thing is now Location 2.
        List<Location> thingLocations = thing.locations().query().list().toList();
        Assert.assertEquals(thingLocations.size(), 1, "Incorrect number of Locations for Thing.");
        Assert.assertEquals(thingLocations.get(0), location2);

        // Now create a new HistoricalLocation for the Thing, with an earlier time.
        HistoricalLocation histLocation3 = HistoricalLocationBuilder.builder()
                .location(location3)
                .time(ZonedDateTime.parse("2016-01-01T05:00:00.000Z"))
                .thing(thing.withOnlyId())
                .build();
        service.create(histLocation3);

        // Check if the Location of the Thing is still Location 2.
        thingLocations = thing.locations().query().list().toList();
        Assert.assertEquals(thingLocations.size(), 1, "Incorrect number of Locations for Thing.");
        Assert.assertEquals(thingLocations.get(0), location2);
    }

}

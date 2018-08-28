package org.opengis.cite.sta10.filteringExtension;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.dao.ObservationDao;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.TimeObject;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
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
import org.threeten.extra.Interval;

/**
 * Tests date and time functions.
 *
 * @author Hylke van der Schaaf
 */
public class DateTimeTests {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(DateTimeTests.class);
    private String rootUri;
    private SensorThingsService service;
    private final List<Thing> THINGS = new ArrayList<>();
    private final List<Observation> OBSERVATIONS = new ArrayList<>();
    private ZonedDateTime T2015;
    private ZonedDateTime T600;
    private ZonedDateTime T659;
    private ZonedDateTime T700;
    private ZonedDateTime T701;
    private ZonedDateTime T759;
    private ZonedDateTime T800;
    private ZonedDateTime T801;
    private ZonedDateTime T900;
    private ZonedDateTime T2017;
    private Interval I2015;
    private Interval I600_659;
    private Interval I600_700;
    private Interval I600_701;
    private Interval I700_800;
    private Interval I701_759;
    private Interval I759_900;
    private Interval I800_900;
    private Interval I801_900;
    private Interval I659_801;
    private Interval I700_759;
    private Interval I700_801;
    private Interval I659_800;
    private Interval I701_800;
    private Interval I2017;

    public DateTimeTests() {
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
        THINGS.add(thing);
        Location location = new Location("Location 1.0", "Location of Thing 1.", "application/vnd.geo+json", new Point(8, 51));
        thing.getLocations().add(location);
        service.create(thing);

        Sensor sensor = new Sensor("Sensor 1", "The first sensor.", "text", "Some metadata.");
        ObservedProperty obsProp = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
        Datastream datastream = new Datastream("Datastream 1", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
        datastream.setThing(thing);
        datastream.setSensor(sensor);
        datastream.setObservedProperty(obsProp);
        service.create(datastream);

        T2015 = ZonedDateTime.parse("2015-01-01T06:00:00.000Z");
        T600 = ZonedDateTime.parse("2016-01-01T06:00:00.000Z");
        T659 = ZonedDateTime.parse("2016-01-01T06:59:00.000Z");
        T700 = ZonedDateTime.parse("2016-01-01T07:00:00.000Z");
        T701 = ZonedDateTime.parse("2016-01-01T07:01:00.000Z");
        T759 = ZonedDateTime.parse("2016-01-01T07:59:00.000Z");
        T800 = ZonedDateTime.parse("2016-01-01T08:00:00.000Z");
        T801 = ZonedDateTime.parse("2016-01-01T08:01:00.000Z");
        T900 = ZonedDateTime.parse("2016-01-01T09:00:00.000Z");
        T2017 = ZonedDateTime.parse("2017-01-01T09:00:00.000Z");

        I2015 = Interval.of(T2015.toInstant(), T2015.plus(1, ChronoUnit.HOURS).toInstant());
        I600_659 = Interval.of(T600.toInstant(), T659.toInstant());
        I600_700 = Interval.of(T600.toInstant(), T700.toInstant());
        I600_701 = Interval.of(T600.toInstant(), T701.toInstant());
        I700_800 = Interval.of(T700.toInstant(), T800.toInstant());
        I701_759 = Interval.of(T701.toInstant(), T759.toInstant());
        I759_900 = Interval.of(T759.toInstant(), T900.toInstant());
        I800_900 = Interval.of(T800.toInstant(), T900.toInstant());
        I801_900 = Interval.of(T801.toInstant(), T900.toInstant());
        I659_801 = Interval.of(T659.toInstant(), T801.toInstant());
        I700_759 = Interval.of(T700.toInstant(), T759.toInstant());
        I700_801 = Interval.of(T700.toInstant(), T801.toInstant());
        I659_800 = Interval.of(T659.toInstant(), T800.toInstant());
        I701_800 = Interval.of(T701.toInstant(), T800.toInstant());
        I2017 = Interval.of(T2017.toInstant(), T2017.plus(1, ChronoUnit.HOURS).toInstant());

        createObservation(0, datastream, T600, T600, null); // 0
        createObservation(1, datastream, T659, T659, null); // 1
        createObservation(2, datastream, T700, T700, null); // 2
        createObservation(3, datastream, T701, T701, null); // 3
        createObservation(4, datastream, T759, T759, null); // 4
        createObservation(5, datastream, T800, T800, null); // 5
        createObservation(6, datastream, T801, T801, null); // 6
        createObservation(7, datastream, T900, T900, null); // 7

        createObservation(8, datastream, I600_659, null, I600_659); // 8
        createObservation(9, datastream, I600_700, null, I600_700); // 9
        createObservation(10, datastream, I600_701, null, I600_701); // 10
        createObservation(11, datastream, I700_800, null, I700_800); // 11
        createObservation(12, datastream, I701_759, null, I701_759); // 12
        createObservation(13, datastream, I759_900, null, I759_900); // 13
        createObservation(14, datastream, I800_900, null, I800_900); // 14
        createObservation(15, datastream, I801_900, null, I801_900); // 15

        createObservation(16, datastream, I659_801, null, I659_801); // 16
        createObservation(17, datastream, I700_759, null, I700_759); // 17
        createObservation(18, datastream, I700_801, null, I700_801); // 18
        createObservation(19, datastream, I659_800, null, I659_800); // 19
        createObservation(20, datastream, I701_800, null, I701_800); // 20

        createObservation(21, datastream, T2015, T2015, null); // 21
        createObservation(22, datastream, T2017, T2017, null); // 22
        createObservation(23, datastream, I2015, null, I2015); // 23
        createObservation(24, datastream, I2017, null, I2017); // 24
    }

    private void createObservation(double result, Datastream ds, Interval pt, ZonedDateTime rt, Interval vt) throws ServiceFailureException {
        createObservation(result, ds, new TimeObject(pt), rt, vt);
    }

    private void createObservation(double result, Datastream ds, ZonedDateTime pt, ZonedDateTime rt, Interval vt) throws ServiceFailureException {
        createObservation(result, ds, new TimeObject(pt), rt, vt);
    }

    private void createObservation(double result, Datastream ds, TimeObject pt, ZonedDateTime rt, Interval vt) throws ServiceFailureException {
        Observation o = new Observation(result, ds);
        o.setPhenomenonTime(pt);
        o.setResultTime(rt);
        o.setValidTime(vt);
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

    public void filterForException(BaseDao doa, String filter) {
        try {
            doa.query().filter(filter).list();
        } catch (IllegalArgumentException e) {
            return;
        } catch (ServiceFailureException ex) {
            Assert.fail("Failed to call service.", ex);
        }
        Assert.fail("Filter " + filter + " did not respond with 400 Bad Request.");
    }

    @Test(description = "Test lt operator on times and time intervals.", groups = "level-3")
    public void testLt() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("resultTime lt %s", T700), getFromList(OBSERVATIONS, 0, 1, 21));
        filterAndCheck(doa, String.format("validTime lt %s", T700), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("phenomenonTime lt %s", T700), getFromList(OBSERVATIONS, 0, 1, 8, 9, 21, 23));

        filterAndCheck(doa, String.format("resultTime lt %s", I700_800), getFromList(OBSERVATIONS, 0, 1, 21));
        filterAndCheck(doa, String.format("validTime lt %s", I700_800), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("phenomenonTime lt %s", I700_800), getFromList(OBSERVATIONS, 0, 1, 8, 9, 21, 23));

        filterAndCheck(doa, String.format("%s lt resultTime", T800), getFromList(OBSERVATIONS, 6, 7, 22));
        filterAndCheck(doa, String.format("%s lt validTime", T800), getFromList(OBSERVATIONS, 15, 24));
        filterAndCheck(doa, String.format("%s lt phenomenonTime", T800), getFromList(OBSERVATIONS, 6, 7, 15, 22, 24));

        filterAndCheck(doa, String.format("%s lt resultTime", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 22));
        filterAndCheck(doa, String.format("%s lt validTime", I700_800), getFromList(OBSERVATIONS, 14, 15, 24));
        filterAndCheck(doa, String.format("%s lt phenomenonTime", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 14, 15, 22, 24));
    }

    @Test(description = "Test gt operator on times and time intervals.", groups = "level-3")
    public void testGt() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("resultTime gt %s", T800), getFromList(OBSERVATIONS, 6, 7, 22));
        filterAndCheck(doa, String.format("validTime gt %s", T800), getFromList(OBSERVATIONS, 15, 24));
        filterAndCheck(doa, String.format("phenomenonTime gt %s", T800), getFromList(OBSERVATIONS, 6, 7, 15, 22, 24));

        filterAndCheck(doa, String.format("resultTime gt %s", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 22));
        filterAndCheck(doa, String.format("validTime gt %s", I700_800), getFromList(OBSERVATIONS, 14, 15, 24));
        filterAndCheck(doa, String.format("phenomenonTime gt %s", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 14, 15, 22, 24));

        filterAndCheck(doa, String.format("%s gt resultTime", T700), getFromList(OBSERVATIONS, 0, 1, 21));
        filterAndCheck(doa, String.format("%s gt validTime", T700), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("%s gt phenomenonTime", T700), getFromList(OBSERVATIONS, 0, 1, 8, 9, 21, 23));

        filterAndCheck(doa, String.format("%s gt resultTime", I700_800), getFromList(OBSERVATIONS, 0, 1, 21));
        filterAndCheck(doa, String.format("%s gt validTime", I700_800), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("%s gt phenomenonTime", I700_800), getFromList(OBSERVATIONS, 0, 1, 8, 9, 21, 23));
    }

    @Test(description = "Test le operator on times and time intervals.", groups = "level-3")
    public void testLe() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("resultTime le %s", T700), getFromList(OBSERVATIONS, 0, 1, 2, 21));
        filterAndCheck(doa, String.format("validTime le %s", T700), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("phenomenonTime le %s", T700), getFromList(OBSERVATIONS, 0, 1, 2, 8, 9, 21, 23));

        filterAndCheck(doa, String.format("resultTime le %s", I700_800), getFromList(OBSERVATIONS, 0, 1, 2, 21));
        filterAndCheck(doa, String.format("validTime le %s", I700_800), getFromList(OBSERVATIONS, 8, 9, 10, 11, 17, 19, 23));
        filterAndCheck(doa, String.format("phenomenonTime le %s", I700_800), getFromList(OBSERVATIONS, 0, 1, 2, 8, 9, 10, 11, 17, 19, 21, 23));

        filterAndCheck(doa, String.format("%s le resultTime", T800), getFromList(OBSERVATIONS, 5, 6, 7, 22));
        filterAndCheck(doa, String.format("%s le validTime", T800), getFromList(OBSERVATIONS, 14, 15, 24));
        filterAndCheck(doa, String.format("%s le phenomenonTime", T800), getFromList(OBSERVATIONS, 5, 6, 7, 14, 15, 22, 24));

        filterAndCheck(doa, String.format("%s le resultTime", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 22));
        filterAndCheck(doa, String.format("%s le validTime", I700_800), getFromList(OBSERVATIONS, 11, 13, 14, 15, 18, 20, 24));
        filterAndCheck(doa, String.format("%s le phenomenonTime", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 11, 13, 14, 15, 18, 20, 22, 24));
    }

    @Test(description = "Test ge operator on times and time intervals.", groups = "level-3")
    public void testGe() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("resultTime ge %s", T800), getFromList(OBSERVATIONS, 5, 6, 7, 22));
        filterAndCheck(doa, String.format("validTime ge %s", T800), getFromList(OBSERVATIONS, 14, 15, 24));
        filterAndCheck(doa, String.format("phenomenonTime ge %s", T800), getFromList(OBSERVATIONS, 5, 6, 7, 14, 15, 22, 24));

        filterAndCheck(doa, String.format("resultTime ge %s", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 22));
        filterAndCheck(doa, String.format("validTime ge %s", I700_800), getFromList(OBSERVATIONS, 11, 13, 14, 15, 18, 20, 24));
        filterAndCheck(doa, String.format("phenomenonTime ge %s", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 11, 13, 14, 15, 18, 20, 22, 24));

        filterAndCheck(doa, String.format("%s ge resultTime", T700), getFromList(OBSERVATIONS, 0, 1, 2, 21));
        filterAndCheck(doa, String.format("%s ge validTime", T700), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("%s ge phenomenonTime", T700), getFromList(OBSERVATIONS, 0, 1, 2, 8, 9, 21, 23));

        filterAndCheck(doa, String.format("%s ge resultTime", I700_800), getFromList(OBSERVATIONS, 0, 1, 2, 21));
        filterAndCheck(doa, String.format("%s ge validTime", I700_800), getFromList(OBSERVATIONS, 8, 9, 10, 11, 17, 19, 23));
        filterAndCheck(doa, String.format("%s ge phenomenonTime", I700_800), getFromList(OBSERVATIONS, 0, 1, 2, 8, 9, 10, 11, 17, 19, 21, 23));
    }

    @Test(description = "Test eq operator on times and time intervals.", groups = "level-3")
    public void testEq() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("resultTime eq %s", T800), getFromList(OBSERVATIONS, 5));
        filterAndCheck(doa, String.format("validTime eq %s", T800), getFromList(OBSERVATIONS));
        filterAndCheck(doa, String.format("phenomenonTime eq %s", T800), getFromList(OBSERVATIONS, 5));

        filterAndCheck(doa, String.format("resultTime eq %s", I700_800), getFromList(OBSERVATIONS));
        filterAndCheck(doa, String.format("validTime eq %s", I700_800), getFromList(OBSERVATIONS, 11));
        filterAndCheck(doa, String.format("phenomenonTime eq %s", I700_800), getFromList(OBSERVATIONS, 11));

        filterAndCheck(doa, String.format("%s eq resultTime", T700), getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, String.format("%s eq validTime", T700), getFromList(OBSERVATIONS));
        filterAndCheck(doa, String.format("%s eq phenomenonTime", T700), getFromList(OBSERVATIONS, 2));

        filterAndCheck(doa, String.format("%s eq resultTime", I700_800), getFromList(OBSERVATIONS));
        filterAndCheck(doa, String.format("%s eq validTime", I700_800), getFromList(OBSERVATIONS, 11));
        filterAndCheck(doa, String.format("%s eq phenomenonTime", I700_800), getFromList(OBSERVATIONS, 11));
    }

    @Test(description = "Test the before() function.", groups = "level-3")
    public void testBefore() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("before(resultTime,%s)", T700), getFromList(OBSERVATIONS, 0, 1, 21));
        filterAndCheck(doa, String.format("before(validTime,%s)", T700), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("before(phenomenonTime,%s)", T700), getFromList(OBSERVATIONS, 0, 1, 8, 9, 21, 23));

        filterAndCheck(doa, String.format("before(resultTime,%s)", I700_800), getFromList(OBSERVATIONS, 0, 1, 21));
        filterAndCheck(doa, String.format("before(validTime,%s)", I700_800), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("before(phenomenonTime,%s)", I700_800), getFromList(OBSERVATIONS, 0, 1, 8, 9, 21, 23));

        filterAndCheck(doa, String.format("before(%s,resultTime)", T800), getFromList(OBSERVATIONS, 6, 7, 22));
        filterAndCheck(doa, String.format("before(%s,validTime)", T800), getFromList(OBSERVATIONS, 15, 24));
        filterAndCheck(doa, String.format("before(%s,phenomenonTime)", T800), getFromList(OBSERVATIONS, 6, 7, 15, 22, 24));

        filterAndCheck(doa, String.format("before(%s,resultTime)", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 22));
        filterAndCheck(doa, String.format("before(%s,validTime)", I700_800), getFromList(OBSERVATIONS, 14, 15, 24));
        filterAndCheck(doa, String.format("before(%s,phenomenonTime)", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 14, 15, 22, 24));
    }

    @Test(description = "Test the after() function.", groups = "level-3")
    public void testAfter() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("after(resultTime,%s)", T800), getFromList(OBSERVATIONS, 6, 7, 22));
        filterAndCheck(doa, String.format("after(validTime,%s)", T800), getFromList(OBSERVATIONS, 15, 24));
        filterAndCheck(doa, String.format("after(phenomenonTime,%s)", T800), getFromList(OBSERVATIONS, 6, 7, 15, 22, 24));

        filterAndCheck(doa, String.format("after(resultTime,%s)", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 22));
        filterAndCheck(doa, String.format("after(validTime,%s)", I700_800), getFromList(OBSERVATIONS, 14, 15, 24));
        filterAndCheck(doa, String.format("after(phenomenonTime,%s)", I700_800), getFromList(OBSERVATIONS, 5, 6, 7, 14, 15, 22, 24));

        filterAndCheck(doa, String.format("after(%s,resultTime)", T700), getFromList(OBSERVATIONS, 0, 1, 21));
        filterAndCheck(doa, String.format("after(%s,validTime)", T700), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("after(%s,phenomenonTime)", T700), getFromList(OBSERVATIONS, 0, 1, 8, 9, 21, 23));

        filterAndCheck(doa, String.format("after(%s,resultTime)", I700_800), getFromList(OBSERVATIONS, 0, 1, 21));
        filterAndCheck(doa, String.format("after(%s,validTime)", I700_800), getFromList(OBSERVATIONS, 8, 9, 23));
        filterAndCheck(doa, String.format("after(%s,phenomenonTime)", I700_800), getFromList(OBSERVATIONS, 0, 1, 8, 9, 21, 23));
    }

    @Test(description = "Test the meets() function.", groups = "level-3")
    public void testMeets() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("meets(resultTime,%s)", T700), getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, String.format("meets(validTime,%s)", T700), getFromList(OBSERVATIONS, 9, 11, 17, 18));
        filterAndCheck(doa, String.format("meets(phenomenonTime,%s)", T700), getFromList(OBSERVATIONS, 2, 9, 11, 17, 18));

        filterAndCheck(doa, String.format("meets(resultTime,%s)", I700_800), getFromList(OBSERVATIONS, 2, 5));
        filterAndCheck(doa, String.format("meets(validTime,%s)", I700_800), getFromList(OBSERVATIONS, 9, 14));
        filterAndCheck(doa, String.format("meets(phenomenonTime,%s)", I700_800), getFromList(OBSERVATIONS, 2, 5, 9, 14));

        filterAndCheck(doa, String.format("meets(%s,resultTime)", T700), getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, String.format("meets(%s,validTime)", T700), getFromList(OBSERVATIONS, 9, 11, 17, 18));
        filterAndCheck(doa, String.format("meets(%s,phenomenonTime)", T700), getFromList(OBSERVATIONS, 2, 9, 11, 17, 18));

        filterAndCheck(doa, String.format("meets(%s,resultTime)", I700_800), getFromList(OBSERVATIONS, 2, 5));
        filterAndCheck(doa, String.format("meets(%s,validTime)", I700_800), getFromList(OBSERVATIONS, 9, 14));
        filterAndCheck(doa, String.format("meets(%s,phenomenonTime)", I700_800), getFromList(OBSERVATIONS, 2, 5, 9, 14));
    }

    @Test(description = "Test the during() function.", groups = "level-3")
    public void testDuring() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterForException(doa, String.format("during(resultTime,%s)", T700));
        filterForException(doa, String.format("during(validTime,%s)", T700));
        filterForException(doa, String.format("during(phenomenonTime,%s)", T700));

        filterAndCheck(doa, String.format("during(resultTime,%s)", I700_800), getFromList(OBSERVATIONS, 2, 3, 4));
        filterAndCheck(doa, String.format("during(validTime,%s)", I700_800), getFromList(OBSERVATIONS, 11, 12, 17, 20));
        filterAndCheck(doa, String.format("during(phenomenonTime,%s)", I700_800), getFromList(OBSERVATIONS, 2, 3, 4, 11, 12, 17, 20));

        filterForException(doa, String.format("during(%s,resultTime)", T700));
        filterAndCheck(doa, String.format("during(%s,validTime)", T700), getFromList(OBSERVATIONS, 10, 11, 16, 17, 18, 19));
        filterAndCheck(doa, String.format("during(%s,phenomenonTime)", T700), getFromList(OBSERVATIONS, 10, 11, 16, 17, 18, 19));

        filterForException(doa, String.format("during(%s,resultTime)", I700_800));
        filterAndCheck(doa, String.format("during(%s,validTime)", I700_800), getFromList(OBSERVATIONS, 11, 16, 18, 19));
        filterAndCheck(doa, String.format("during(%s,phenomenonTime)", I700_800), getFromList(OBSERVATIONS, 11, 16, 18, 19));
    }

    @Test(description = "Test the overlaps() function.", groups = "level-3")
    public void testOverlaps() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("overlaps(resultTime,%s)", T700), getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, String.format("overlaps(validTime,%s)", T700), getFromList(OBSERVATIONS, 10, 11, 16, 17, 18, 19));
        filterAndCheck(doa, String.format("overlaps(phenomenonTime,%s)", T700), getFromList(OBSERVATIONS, 2, 10, 11, 16, 17, 18, 19));

        filterAndCheck(doa, String.format("overlaps(resultTime,%s)", I700_800), getFromList(OBSERVATIONS, 2, 3, 4));
        filterAndCheck(doa, String.format("overlaps(validTime,%s)", I700_800), getFromList(OBSERVATIONS, 10, 11, 12, 13, 16, 17, 18, 19, 20));
        filterAndCheck(doa, String.format("overlaps(phenomenonTime,%s)", I700_800), getFromList(OBSERVATIONS, 2, 3, 4, 10, 11, 12, 13, 16, 17, 18, 19, 20));

        filterAndCheck(doa, String.format("overlaps(%s,resultTime)", T700), getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, String.format("overlaps(%s,validTime)", T700), getFromList(OBSERVATIONS, 10, 11, 16, 17, 18, 19));
        filterAndCheck(doa, String.format("overlaps(%s,phenomenonTime)", T700), getFromList(OBSERVATIONS, 2, 10, 11, 16, 17, 18, 19));

        filterAndCheck(doa, String.format("overlaps(%s,resultTime)", I700_800), getFromList(OBSERVATIONS, 2, 3, 4));
        filterAndCheck(doa, String.format("overlaps(%s,validTime)", I700_800), getFromList(OBSERVATIONS, 10, 11, 12, 13, 16, 17, 18, 19, 20));
        filterAndCheck(doa, String.format("overlaps(%s,phenomenonTime)", I700_800), getFromList(OBSERVATIONS, 2, 3, 4, 10, 11, 12, 13, 16, 17, 18, 19, 20));
    }

    @Test(description = "Test the starts() function.", groups = "level-3")
    public void testStarts() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("starts(resultTime,%s)", T700), getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, String.format("starts(validTime,%s)", T700), getFromList(OBSERVATIONS, 11, 17, 18));
        filterAndCheck(doa, String.format("starts(phenomenonTime,%s)", T700), getFromList(OBSERVATIONS, 2, 11, 17, 18));

        filterAndCheck(doa, String.format("starts(resultTime,%s)", I700_800), getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, String.format("starts(validTime,%s)", I700_800), getFromList(OBSERVATIONS, 11, 17, 18));
        filterAndCheck(doa, String.format("starts(phenomenonTime,%s)", I700_800), getFromList(OBSERVATIONS, 2, 11, 17, 18));

        filterAndCheck(doa, String.format("starts(%s,resultTime)", T700), getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, String.format("starts(%s,validTime)", T700), getFromList(OBSERVATIONS, 11, 17, 18));
        filterAndCheck(doa, String.format("starts(%s,phenomenonTime)", T700), getFromList(OBSERVATIONS, 2, 11, 17, 18));

        filterAndCheck(doa, String.format("starts(%s,resultTime)", I700_800), getFromList(OBSERVATIONS, 2));
        filterAndCheck(doa, String.format("starts(%s,validTime)", I700_800), getFromList(OBSERVATIONS, 11, 17, 18));
        filterAndCheck(doa, String.format("starts(%s,phenomenonTime)", I700_800), getFromList(OBSERVATIONS, 2, 11, 17, 18));
    }

    @Test(description = "Test the finishes() function.", groups = "level-3")
    public void testFinishes() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("finishes(resultTime,%s)", T800), getFromList(OBSERVATIONS, 5));
        filterAndCheck(doa, String.format("finishes(validTime,%s)", T800), getFromList(OBSERVATIONS, 11, 19, 20));
        filterAndCheck(doa, String.format("finishes(phenomenonTime,%s)", T800), getFromList(OBSERVATIONS, 5, 11, 19, 20));

        filterAndCheck(doa, String.format("finishes(resultTime,%s)", I700_800), getFromList(OBSERVATIONS, 5));
        filterAndCheck(doa, String.format("finishes(validTime,%s)", I700_800), getFromList(OBSERVATIONS, 11, 19, 20));
        filterAndCheck(doa, String.format("finishes(phenomenonTime,%s)", I700_800), getFromList(OBSERVATIONS, 5, 11, 19, 20));

        filterAndCheck(doa, String.format("finishes(%s,resultTime)", T800), getFromList(OBSERVATIONS, 5));
        filterAndCheck(doa, String.format("finishes(%s,validTime)", T800), getFromList(OBSERVATIONS, 11, 19, 20));
        filterAndCheck(doa, String.format("finishes(%s,phenomenonTime)", T800), getFromList(OBSERVATIONS, 5, 11, 19, 20));

        filterAndCheck(doa, String.format("finishes(%s,resultTime)", I700_800), getFromList(OBSERVATIONS, 5));
        filterAndCheck(doa, String.format("finishes(%s,validTime)", I700_800), getFromList(OBSERVATIONS, 11, 19, 20));
        filterAndCheck(doa, String.format("finishes(%s,phenomenonTime)", I700_800), getFromList(OBSERVATIONS, 5, 11, 19, 20));
    }

    @Test(description = "Test the year() function.", groups = "level-3")
    public void testYear() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("year(resultTime) eq 2015"), getFromList(OBSERVATIONS, 21));
        filterAndCheck(doa, String.format("year(validTime) eq 2015"), getFromList(OBSERVATIONS, 23));
        filterAndCheck(doa, String.format("year(phenomenonTime) eq 2015"), getFromList(OBSERVATIONS, 21, 23));
    }

    public static <T extends Entity<T>> List<T> getFromList(List<T> list, int... ids) {
        List<T> result = new ArrayList<>();
        for (int i : ids) {
            result.add(list.get(i));
        }
        return result;
    }

    public static <T extends Entity<T>> List<T> getFromListExcept(List<T> list, int... ids) {
        List<T> result = new ArrayList<>(list);
        for (int i : ids) {
            result.remove(list.get(i));
        }
        return result;
    }

    public static <T extends Entity<T>> List<T> removeFromList(List<T> sourceList, List<T> remaining, int... ids) {
        for (int i : ids) {
            remaining.remove(sourceList.get(i));
        }
        return remaining;
    }
}

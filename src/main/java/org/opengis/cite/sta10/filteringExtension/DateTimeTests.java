/*
 * Copyright (C) 2016 Hylke van der Schaaf.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library. If not, see <http://www.gnu.org/licenses/>.
 */
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
    private ZonedDateTime T600;
    private ZonedDateTime T659;
    private ZonedDateTime T700;
    private ZonedDateTime T701;
    private ZonedDateTime T759;
    private ZonedDateTime T800;
    private ZonedDateTime T801;
    private ZonedDateTime T900;
    private Interval I600_659;
    private Interval I600_700;
    private Interval I600_701;
    private Interval I700_800;
    private Interval I701_759;
    private Interval I759_900;
    private Interval I800_900;
    private Interval I801_900;

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

        T600 = ZonedDateTime.parse("2016-01-01T06:00:00.000Z");
        T659 = ZonedDateTime.parse("2016-01-01T06:59:00.000Z");
        T700 = ZonedDateTime.parse("2016-01-01T07:00:00.000Z");
        T701 = ZonedDateTime.parse("2016-01-01T07:01:00.000Z");
        T759 = ZonedDateTime.parse("2016-01-01T07:59:00.000Z");
        T800 = ZonedDateTime.parse("2016-01-01T08:00:00.000Z");
        T801 = ZonedDateTime.parse("2016-01-01T08:01:00.000Z");
        T900 = ZonedDateTime.parse("2016-01-01T09:00:00.000Z");

        I600_659 = Interval.of(T600.toInstant(), T659.toInstant());
        I600_700 = Interval.of(T600.toInstant(), T700.toInstant());
        I600_701 = Interval.of(T600.toInstant(), T701.toInstant());
        I700_800 = Interval.of(T700.toInstant(), T800.toInstant());
        I701_759 = Interval.of(T701.toInstant(), T759.toInstant());
        I759_900 = Interval.of(T759.toInstant(), T900.toInstant());
        I800_900 = Interval.of(T800.toInstant(), T900.toInstant());
        I801_900 = Interval.of(T801.toInstant(), T900.toInstant());

        createObservation(1, datastream, T600, null, I600_659);
        createObservation(1, datastream, T659, null, I600_700);
        createObservation(1, datastream, T700, null, I600_701);
        createObservation(1, datastream, T701, null, I700_800);
        createObservation(1, datastream, T759, null, I701_759);
        createObservation(1, datastream, T800, null, I759_900);
        createObservation(1, datastream, T801, null, I800_900);
        createObservation(1, datastream, T900, null, I801_900);

        createObservation(1, datastream, I600_659, T600, null);
        createObservation(1, datastream, I600_700, T659, null);
        createObservation(1, datastream, I600_701, T700, null);
        createObservation(1, datastream, I700_800, T701, null);
        createObservation(1, datastream, I701_759, T759, null);
        createObservation(1, datastream, I759_900, T800, null);
        createObservation(1, datastream, I800_900, T801, null);
        createObservation(1, datastream, I801_900, T900, null);
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

    @Test(description = "Test the before() function.", groups = "level-3")
    public void testBefore() throws ServiceFailureException {
        ObservationDao doa = service.observations();
        filterAndCheck(doa, String.format("before(resultTime, %s)", T700), getFromList(OBSERVATIONS, 8, 9));

    }

    public static <T extends Entity<T>> List<T> getFromList(List<T> list, int... ids) {
        List<T> result = new ArrayList<>();
        for (int i : ids) {
            result.add(list.get(i));
        }
        return result;
    }
}

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
package org.opengis.cite.sta10.createUpdateDelete;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jackson.jsonpointer.JsonPointer;
import com.github.fge.jackson.jsonpointer.JsonPointerException;
import com.github.fge.jsonpatch.AddOperation;
import com.github.fge.jsonpatch.CopyOperation;
import com.github.fge.jsonpatch.JsonPatchOperation;
import com.github.fge.jsonpatch.MoveOperation;
import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.model.Datastream;
import de.fraunhofer.iosb.ilt.sta.model.Location;
import de.fraunhofer.iosb.ilt.sta.model.Observation;
import de.fraunhofer.iosb.ilt.sta.model.ObservedProperty;
import de.fraunhofer.iosb.ilt.sta.model.Sensor;
import de.fraunhofer.iosb.ilt.sta.model.Thing;
import de.fraunhofer.iosb.ilt.sta.model.ext.UnitOfMeasurement;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.geojson.Point;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.cite.sta10.SuiteAttribute;
import org.opengis.cite.sta10.util.EntityType;
import org.opengis.cite.sta10.util.HTTPMethods;
import org.opengis.cite.sta10.util.ServiceURLBuilder;
import org.opengis.cite.sta10.util.Utils;
import org.testng.Assert;
import org.testng.ITestContext;
import org.testng.annotations.Test;

/**
 * TODO.
 * <ul>
 * <li>geo.length</li>
 * </ul>
 *
 * @author Hylke van der Schaaf
 */
public class JsonPatchTests {

    private static SensorThingsService service;
    private static final List<Thing> THINGS = new ArrayList<>();
    private static final List<Location> LOCATIONS = new ArrayList<>();
    private static final List<Sensor> SENSORS = new ArrayList<>();
    private static final List<ObservedProperty> OPROPS = new ArrayList<>();
    private static final List<Datastream> DATASTREAMS = new ArrayList<>();
    private static final List<Observation> OBSERVATIONS = new ArrayList<>();
    /**
     * The root URL of the SensorThings service under the test
     */
    public String rootUri;

    /**
     * This method will be run before starting the test for this conformance
     * class. It cleans the database to start test.
     *
     * @param testContext The test context to find out whether this class is
     *                    requested to test or not
     * @throws java.net.MalformedURLException If the url passed to the test is
     */
    @org.testng.annotations.BeforeClass
    public void obtainTestSubject(ITestContext testContext) throws MalformedURLException, ServiceFailureException, URISyntaxException {
        Object obj = testContext.getSuite().getAttribute(
                SuiteAttribute.LEVEL.getName());
        if ((null != obj)) {
            Integer level = Integer.class.cast(obj);
            org.testng.Assert.assertTrue(level > 1,
                    "Conformance level 2 will not be checked since ics = " + level);
        }

        rootUri = testContext.getSuite().getAttribute(
                SuiteAttribute.TEST_SUBJECT.getName()).toString();
        rootUri = rootUri.trim();
        if (rootUri.lastIndexOf('/') == rootUri.length() - 1) {
            rootUri = rootUri.substring(0, rootUri.length() - 1);
        }
        service = new SensorThingsService(new URL(rootUri));
        deleteEverything();
        createEntities();
    }

    /**
     * This method is run after all the tests of this class is run and clean the
     * database.
     *
     * @throws de.fraunhofer.iosb.ilt.sta.ServiceFailureException
     */
    @org.testng.annotations.AfterClass
    public void deleteEverything() throws ServiceFailureException {
        Utils.deleteAll(service);
    }

    private static void createEntities() throws ServiceFailureException, URISyntaxException {
        {
            Thing thing = new Thing("Thing 1", "The first thing.");
            service.create(thing);
            THINGS.add(thing);
        }
        {
            Location location = new Location("Location Des Dings von ILT", "First Location of Thing 1.", "application/vnd.geo+json", new Point(8, 49));
            location.getThings().add(THINGS.get(0));
            service.create(location);
            LOCATIONS.add(location);
        }
        {
            Sensor sensor1 = new Sensor("Sensor 1", "The first sensor.", "text", "Some metadata.");
            service.create(sensor1);
            SENSORS.add(sensor1);
        }
        {
            Sensor sensor2 = new Sensor("Sensor 2", "The second sensor", "text", "Some metadata.");
            service.create(sensor2);
            SENSORS.add(sensor2);
        }
        {
            ObservedProperty obsProp1 = new ObservedProperty("Temperature", new URI("http://ucom.org/temperature"), "The temperature of the thing.");
            service.create(obsProp1);
            OPROPS.add(obsProp1);
        }
        {
            ObservedProperty obsProp2 = new ObservedProperty("Humidity", new URI("http://ucom.org/humidity"), "The humidity of the thing.");
            service.create(obsProp2);
            OPROPS.add(obsProp2);
        }
        {
            Datastream datastream1 = new Datastream("Datastream Temp", "The temperature of thing 1, sensor 1.", "someType", new UnitOfMeasurement("degree celcius", "°C", "ucum:T"));
            datastream1.setThing(THINGS.get(0).withOnlyId());
            datastream1.setSensor(SENSORS.get(0).withOnlyId());
            datastream1.setObservedProperty(OPROPS.get(0).withOnlyId());
            service.create(datastream1);
            DATASTREAMS.add(datastream1);
        }
        {
            Datastream datastream2 = new Datastream("Datastream LF", "The humidity of thing 1, sensor 2.", "someType", new UnitOfMeasurement("relative humidity", "%", "ucum:Humidity"));
            datastream2.setThing(THINGS.get(0).withOnlyId());
            datastream2.setSensor(SENSORS.get(1).withOnlyId());
            datastream2.setObservedProperty(OPROPS.get(1).withOnlyId());
            service.create(datastream2);
            DATASTREAMS.add(datastream2);
        }
    }

    @Test(description = "JSON PATCH copy and move", groups = "level-2", priority = 6)
    public void jsonPatchTest() throws ServiceFailureException, JsonPointerException, IOException {
        Thing thingOnlyId = THINGS.get(0).withOnlyId();
        List<JsonPatchOperation> operations = new ArrayList<>();
        operations.add(new AddOperation(new JsonPointer("/properties"), new ObjectMapper().readTree("{\"key1\": 1}")));
        service.patch(thingOnlyId, operations);
        Thing updatedThing = service.things().find(thingOnlyId.getId());
        Assert.assertEquals(1, updatedThing.getProperties().get("key1"), "properties/key1 was not added correctly.");

        operations.clear();
        operations.add(new CopyOperation(new JsonPointer("/properties/key1"), new JsonPointer("/properties/keyCopy1")));
        operations.add(new MoveOperation(new JsonPointer("/properties/key1"), new JsonPointer("/properties/key2")));
        service.patch(thingOnlyId, operations);
        updatedThing = service.things().find(thingOnlyId.getId());
        Assert.assertEquals(1, updatedThing.getProperties().get("keyCopy1"), "properties/keyCopy1 does not exist after copy.");
        Assert.assertEquals(null, updatedThing.getProperties().get("key1"), "properties/key1 still exists after move.");
        Assert.assertEquals(1, updatedThing.getProperties().get("key2"), "properties/key2 does not exist after move.");

    }

    /**
     * This method created the URL string for the entity with specific id and
     * then PATCH the entity with urlParameters to that URL.
     *
     * @param entityType Entity type in from EntityType enum
     * @param patchBody  The PATCH body
     * @param id         The id of requested entity
     * @return The patched entity in the format of JSON Object
     */
    private JSONObject jsonPatchEntity(EntityType entityType, Object id, String patchBody) {
        String urlString = ServiceURLBuilder.buildURLString(rootUri, entityType, id, null, null);
        try {

            Map<String, Object> responseMap = HTTPMethods.doPatch(urlString, patchBody);
            int responseCode = Integer.parseInt(responseMap.get("response-code").toString());
            Assert.assertEquals(responseCode, 200, "Error during updating(PATCH) of entity " + entityType.name());
            responseMap = HTTPMethods.doGet(urlString);
            JSONObject result = new JSONObject(responseMap.get("response").toString());
            return result;

        } catch (JSONException e) {
            e.printStackTrace();
            Assert.fail("An Exception occurred during testing!:\n" + e.getMessage());
            return null;
        }
    }
}

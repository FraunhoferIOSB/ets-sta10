package org.opengis.cite.sta10;

import com.sun.jersey.api.client.Client;
import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.opengis.cite.sta10.util.ClientUtils;
import org.opengis.cite.sta10.util.TestSuiteLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.ISuite;
import org.testng.ISuiteListener;

/**
 * A listener that performs various tasks before and after a test suite is run,
 * usually concerned with maintaining a shared test suite fixture. Since this
 * listener is loaded using the ServiceLoader mechanism, its methods will be
 * called before those of other suite listeners listed in the test suite
 * definition and before any annotated configuration methods.
 *
 * Attributes set on an ISuite instance are not inherited by constituent test
 * group contexts (ITestContext). However, suite attributes are still accessible
 * from lower contexts.
 *
 * @see org.testng.ISuite ISuite interface
 */
public class SuiteFixtureListener implements ISuiteListener {

    /**
     * The logger for this class.
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(SuiteFixtureListener.class);

    public static final String KEY_HAS_MULTI_DATASTREAM = "hasMultiDatastream";
    public static final String KEY_HAS_ACTUATION = "hasActuation";

    @Override
    public void onStart(ISuite suite) {
        processSuiteParameters(suite);
        registerClientComponent(suite);
    }

    @Override
    public void onFinish(ISuite suite) {
    }

    /**
     * Processes test suite arguments and sets suite attributes accordingly. The
     * entity referenced by the {@link TestRunArg#IUT iut} argument is parsed
     * and the resulting Document is set as the value of the "testSubject"
     * attribute.
     *
     * @param suite An ISuite object representing a TestNG test suite.
     */
    void processSuiteParameters(ISuite suite) {
        Map<String, String> params = suite.getXmlSuite().getParameters();
        TestSuiteLogger.log(Level.CONFIG,
                "Suite parameters\n" + params.toString());

        Integer level = new Integer(1);
        if (null != params.get(TestRunArg.ICS.toString())) {
            try {
                level = Integer.valueOf(params.get(TestRunArg.ICS.toString()));
            } catch (NumberFormatException nfe) { // use default value instead
            }
        }
        suite.setAttribute(SuiteAttribute.LEVEL.getName(), level);

        String iutParam = params.get(TestRunArg.IUT.toString());

        String response = checkServiceRootUri(iutParam, params);
        if (!response.equals("")) {
            throw new IllegalArgumentException(
                    response);
        }
        suite.setAttribute(SuiteAttribute.TEST_SUBJECT.getName(), iutParam);
        suite.setAttribute(SuiteAttribute.MQTT_SERVER.getName(), params.get(TestRunArg.MQTT_SERVER.toString()));
        // defaulting to 30s timeout
        Long mqttTimeout = new Long(30000);
        if (null != params.get(TestRunArg.MQTT_TIMEOUT.toString())) {
            try {
                mqttTimeout = Long.valueOf(params.get(TestRunArg.MQTT_TIMEOUT.toString()));
            } catch (NumberFormatException nfe) { // use default value instead
                mqttTimeout = 30000l;
            }
        }
        suite.setAttribute(SuiteAttribute.MQTT_TIMEOUT.getName(), mqttTimeout);
        if (TestSuiteLogger.isLoggable(Level.FINE)) {
            StringBuilder logMsg = new StringBuilder(
                    "Parsed resource retrieved from ");
            logMsg.append(TestRunArg.IUT).append("\n");
            // logMsg.append(XMLUtils.writeNodeToString(iutDoc));
            TestSuiteLogger.log(Level.FINE, logMsg.toString());
        }
    }

    /**
     * A client component is added to the suite fixture as the value of the
     * {@link SuiteAttribute#CLIENT} attribute; it may be subsequently accessed
     * via the {@link org.testng.ITestContext#getSuite()} method.
     *
     * @param suite The test suite instance.
     */
    void registerClientComponent(ISuite suite) {
        Client client = ClientUtils.buildClient();
        if (null != client) {
            suite.setAttribute(SuiteAttribute.CLIENT.getName(), client);
        }
    }

    /**
     * Checking the service root URL to be compliant with SensorThings API
     *
     * @param rootUri The root URL for the service under test.
     * @param params  The params map for passing if MultiDatastreams is enabled.
     * @return If the root URL of the service is not compliant to SensorThings
     * API, it will return the reason it is not compliant. Otherwise it returns
     * empty String.
     */
    private String checkServiceRootUri(String rootUri, Map<String, String> params) {
        rootUri = rootUri.trim();
        if (rootUri.endsWith("/")) {
            rootUri = rootUri.substring(0, rootUri.length() - 1);
        }
        HttpURLConnection connection = null;
        String response = null;
        //Create connection
        URL url = null;
        try {
            url = new URL(rootUri);

            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Content-Type",
                    "application/json");

            connection.setUseCaches(false);
            connection.setDoOutput(true);

            //Get Response
            InputStream is = connection.getInputStream();
            BufferedReader rd = new BufferedReader(new InputStreamReader(is));
            StringBuilder responseBuilder = new StringBuilder(); // or StringBuffer if not Java 5+
            String line;
            while ((line = rd.readLine()) != null) {
                responseBuilder.append(line);
                responseBuilder.append('\r');
            }
            response = responseBuilder.toString();
            rd.close();
        } catch (IOException e) {
            LOGGER.error("Cannot connect to " + rootUri + ".", e);
            return "Cannot connect to " + rootUri + ".";
        }
        JSONObject jsonResponse = null;
        JSONArray entities = null;
        try {
            jsonResponse = new JSONObject(response);
            entities = jsonResponse.getJSONArray("value");
        } catch (JSONException e) {
            LOGGER.error("The service response for the root URI \"" + rootUri + "\" is not JSON.", e);
            return "The service response for the root URI \"" + rootUri + "\" is not JSON.";
        }
        Map<String, Boolean> addedLinks = new HashMap<>();
        addedLinks.put("Things", false);
        addedLinks.put("Locations", false);
        addedLinks.put("HistoricalLocations", false);
        addedLinks.put("Datastreams", false);
        addedLinks.put("Sensors", false);
        addedLinks.put("Observations", false);
        addedLinks.put("ObservedProperties", false);
        addedLinks.put("FeaturesOfInterest", false);
        for (int i = 0; i < entities.length(); i++) {
            JSONObject entity = null;
            String name, nameUrl;
            try {
                entity = entities.getJSONObject(i);
                if (!entity.has("name")) {
                    return "The name component of Service root URI response is not available.";
                }
                if (!entity.has("url")) {
                    return "The name component of Service root URI response is not available.";
                }
                name = entity.getString("name");
                nameUrl = entity.getString("url");
            } catch (JSONException e) {
                LOGGER.error("The service response for the root URI \"" + rootUri + "\" is not JSON.", e);
                return "The service response for the root URI \"" + rootUri + "\" is not JSON.";
            }
            switch (name) {
                case "Actuators":
                    if (!nameUrl.equals(rootUri + "/Actuators")) {
                        return "The URL for Actuators in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    params.put(KEY_HAS_ACTUATION, Boolean.TRUE.toString());
                    break;
                case "Tasks":
                    if (!nameUrl.equals(rootUri + "/Tasks")) {
                        return "The URL for Tasks in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    params.put(KEY_HAS_ACTUATION, Boolean.TRUE.toString());
                    break;
                case "TaskingCapabilities":
                    if (!nameUrl.equals(rootUri + "/TaskingCapabilities")) {
                        return "The URL for TaskingCapabilities in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    params.put(KEY_HAS_ACTUATION, Boolean.TRUE.toString());
                    break;
                case "Things":
                    if (!nameUrl.equals(rootUri + "/Things")) {
                        return "The URL for Things in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    break;
                case "Locations":
                    if (!nameUrl.equals(rootUri + "/Locations")) {
                        return "The URL for Locations in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    break;
                case "HistoricalLocations":
                    if (!nameUrl.equals(rootUri + "/HistoricalLocations")) {
                        return "The URL for HistoricalLocations in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    break;
                case "Datastreams":
                    if (!nameUrl.equals(rootUri + "/Datastreams")) {
                        return "The URL for Datastreams in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    break;
                case "MultiDatastreams":
                    if (!nameUrl.equals(rootUri + "/MultiDatastreams")) {
                        return "The URL for Datastreams in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    params.put(KEY_HAS_MULTI_DATASTREAM, Boolean.TRUE.toString());
                    break;
                case "Sensors":
                    if (!nameUrl.equals(rootUri + "/Sensors")) {
                        return "The URL for Sensors in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    break;
                case "Observations":
                    if (!nameUrl.equals(rootUri + "/Observations")) {
                        return "The URL for Observations in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    break;
                case "ObservedProperties":
                    if (!nameUrl.equals(rootUri + "/ObservedProperties")) {
                        return "The URL for ObservedProperties in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    break;
                case "FeaturesOfInterest":
                    if (!nameUrl.equals(rootUri + "/FeaturesOfInterest")) {
                        return "The URL for FeaturesOfInterest in Service Root URI is not compliant to SensorThings API.";
                    }
                    addedLinks.put(name, true);
                    break;
                default:
                    return "There is a component in Service Root URI response that is not in SensorThings API : " + name;
            }
        }
        for (String key : addedLinks.keySet()) {
            if (addedLinks.get(key) == false) {
                return "The Service Root URI response does not contain " + key;
            }
        }
        return "";
    }
}

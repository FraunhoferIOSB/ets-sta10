package org.opengis.cite.sta10.util;

import de.fraunhofer.iosb.ilt.sta.ServiceFailureException;
import de.fraunhofer.iosb.ilt.sta.dao.BaseDao;
import de.fraunhofer.iosb.ilt.sta.model.Entity;
import de.fraunhofer.iosb.ilt.sta.model.ext.EntityList;
import de.fraunhofer.iosb.ilt.sta.service.SensorThingsService;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

/**
 * Utility methods for comparing results and cleaning the service.
 *
 * @author Hylke van der Schaaf
 */
public class EntityUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(EntityUtils.class.getName());

    /**
     * Class returned by checks on results. Encapsulates the result of the
     * check, and the message.
     */
    public static class resultTestResult {

        public final boolean testOk;
        public final String message;

        public resultTestResult(boolean testOk, String message) {
            this.testOk = testOk;
            this.message = message;
        }

    }

    public static resultTestResult resultContains(EntityList<? extends Entity> result, Entity... entities) {
        return resultContains(result, new ArrayList(Arrays.asList(entities)));
    }

    /**
     * Checks if the list contains all the given entities exactly once.
     *
     * @param result
     * @param entityList
     * @return
     */
    public static resultTestResult resultContains(EntityList<? extends Entity> result, List<? extends Entity> entityList) {
        long count = result.getCount();
        if (count != -1 && count != entityList.size()) {
            LOGGER.info("Result count ({}) not equal to expected count ({})", count, entityList.size());
            return new resultTestResult(false, "Result count " + count + " not equal to expected count (" + entityList.size() + ")");
        }
        Iterator<? extends Entity> it;
        for (it = result.fullIterator(); it.hasNext();) {
            Entity next = it.next();
            Entity inList = findEntityIn(next, entityList);
            if (!entityList.remove(inList)) {
                LOGGER.info("Entity with id {} found in result that is not expected.", next.getId());
                return new resultTestResult(false, "Entity with id " + next.getId() + " found in result that is not expected.");
            }
        }
        if (!entityList.isEmpty()) {
            LOGGER.info("Expected entity not found in result.");
            return new resultTestResult(false, entityList.size() + " expected entities not in result.");
        }
        return new resultTestResult(true, "Check ok.");
    }

    public static Entity findEntityIn(Entity entity, List<? extends Entity> entities) {
        Long id = entity.getId();
        for (Entity inList : entities) {
            if (Objects.equals(inList.getId(), id)) {
                return inList;
            }
        }
        return null;
    }

    public static void deleteAll(SensorThingsService sts) throws ServiceFailureException {
        deleteAll(sts.things());
        deleteAll(sts.locations());
        deleteAll(sts.sensors());
        deleteAll(sts.featuresOfInterest());
        deleteAll(sts.observedProperties());
        deleteAll(sts.observations());
    }

    public static <T extends Entity> void deleteAll(BaseDao<T> doa) throws ServiceFailureException {
        boolean more = true;
        int count = 0;
        while (more) {
            EntityList<T> entities = doa.query().list();
            if (entities.getCount() > 0) {
                LOGGER.info("{} to go.", entities.getCount());
            } else {
                more = false;
            }
            for (T entity : entities) {
                doa.delete(entity);
                count++;
            }
        }
        LOGGER.info("Deleted {} using {}.", count, doa.getClass().getName());
    }

    /**
     * Checks the given response against the given request.
     *
     * @param response the response object to check.
     * @param request the request to check the response against.
     */
    public static void checkResponse(JSONObject response, Request request) {
        try {
            if (request.isCollection()) {
                checkCollection(response.getJSONArray("value"), request);

                // check count for request
                Query expandQuery = request.getQuery();
                Boolean count = expandQuery.getCount();
                if (count != null) {
                    String countProperty = "@iot.count";
                    if (count) {
                        Assert.assertTrue(response.has(countProperty), "Response should have property " + countProperty + " for request: '" + request.toString() + "'");
                    } else {
                        Assert.assertFalse(response.has(countProperty), "Response should not have property " + countProperty + " for request: '" + request.toString() + "'");
                    }
                }

            } else {
                checkEntity(response, request);
            }
        } catch (JSONException ex) {
            Assert.fail("Failure when checking response of query '" + request.getLastUrl() + "'", ex);
        }
    }

    /**
     * Check a collection from a response, against the given expand as present
     * in the request.
     *
     * @param collection The collection of items to check.
     * @param expand The expand that led to the collection.
     * @throws JSONException if there is a problem with the json.
     */
    public static void checkCollection(JSONArray collection, Expand expand) throws JSONException {
        // todo: check top
        // todo: check skip
        // todo: check nextlink
        // Check entities
        for (int i = 0; i < collection.length(); i++) {
            checkEntity(collection.getJSONObject(i), expand);
        }
        // todo: check orderby
        // todo: check filter
    }

    /**
     * Check the given entity from a response against the given expand.
     *
     * @param entity The entity to check.
     * @param expand The expand that led to the entity.
     * @throws JSONException if there is a problem with the json.
     */
    public static void checkEntity(JSONObject entity, Expand expand) throws JSONException {
        EntityType entityType = expand.getEntityType();
        Query query = expand.getQuery();

        // Check properties & select
        List<String> select = new ArrayList<>(query.getSelect());
        if (select.isEmpty()) {
            select.addAll(entityType.getProperties());
            if (expand.isToplevel()) {
                select.addAll(entityType.getRelations());
            }
        }
        for (String propertyName : entityType.getProperties()) {
            if (select.contains(propertyName)) {
                Assert.assertTrue(entity.has(propertyName), "Entity should have property " + propertyName + " for request: '" + expand.toString() + "'");
            } else {
                Assert.assertFalse(entity.has(propertyName), "Entity should not have property " + propertyName + " for request: '" + expand.toString() + "'");
            }
        }
        for (String relationName : entityType.getRelations()) {
            String propertyName = relationName + "@iot.navigationLink";
            if (select.contains(relationName)) {
                Assert.assertTrue(entity.has(propertyName), "Entity should have property " + propertyName + " for request: '" + expand.toString() + "'");
            } else {
                Assert.assertFalse(entity.has(propertyName), "Entity should not have property " + propertyName + " for request: '" + expand.toString() + "'");
            }
        }

        // Check expand
        List<String> relations = new ArrayList<>(entityType.getRelations());
        for (Expand subExpand : query.getExpand()) {
            PathElement path = subExpand.getPath().get(0);
            String propertyName = path.getPropertyName();
            if (!entity.has(propertyName)) {
                Assert.fail("Entity should have expanded " + propertyName + " for request: '" + expand.toString() + "'");
            }
            if (path.isCollection()) {
                checkCollection(entity.getJSONArray(propertyName), subExpand);
            } else {
                checkEntity(entity.getJSONObject(propertyName), subExpand);
            }
            relations.remove(propertyName);

            // check count for expand
            Query expandQuery = subExpand.getQuery();
            Boolean count = expandQuery.getCount();
            if (subExpand.isCollection() && count != null) {
                String countProperty = propertyName + "@iot.count";
                if (count) {
                    Assert.assertTrue(entity.has(countProperty), "Entity should have property " + countProperty + " for request: '" + expand.toString() + "'");
                } else {
                    Assert.assertFalse(entity.has(countProperty), "Entity should not have property " + countProperty + " for request: '" + expand.toString() + "'");
                }
            }
        }
        for (String propertyName : relations) {
            if (entity.has(propertyName)) {
                Assert.fail("Entity should not have expanded " + propertyName + " for request: '" + expand.toString() + "'");
            }
        }
    }

}

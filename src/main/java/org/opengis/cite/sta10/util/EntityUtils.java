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
     * Find the expected count value for the given request. Can not determine
     * the count for paths like /Datastreams(xxx)/Thing/Locations since the id
     * of the Thing can not be determined from the path.
     *
     * @param request The request to determine the count for.
     * @param entityCounts The object holding the entity counts.
     * @return The expected count for the given request.
     */
    public static long findCountForRequest(Request request, EntityCounts entityCounts) {
        long parentId = -1;
        long count = -1;
        EntityType parentType = null;
        for (PathElement element : request.getPath()) {
            EntityType elementType = element.getEntityType();
            if (element.getId() != null) {
                parentId = element.getId();
                parentType = elementType;
                count = -1;
            } else if (parentType == null) {
                if (!element.isCollection()) {
                    throw new IllegalArgumentException("Non-collection requested without parent.");
                }
                count = entityCounts.getCount(elementType);
            } else if (element.isCollection()) {
                count = entityCounts.getCount(parentType, parentId, elementType);
                parentType = null;
                parentId = -1;
            } else {
                count = -1;
                // Can not determine the id of this single-entity.
            }
        }

        return count;
    }

    /**
     * Checks the given response against the given request.
     *
     * @param response The response object to check.
     * @param request The request to check the response against.
     * @param entityCounts The object with the expected entity counts.
     */
    public static void checkResponse(JSONObject response, Request request, EntityCounts entityCounts) {
        try {
            if (request.isCollection()) {
                checkCollection(response.getJSONArray("value"), request, entityCounts);

                // check count for request
                Query expandQuery = request.getQuery();
                Boolean count = expandQuery.getCount();
                String countProperty = "@iot.count";
                if (count != null) {
                    if (count) {
                        Assert.assertTrue(response.has(countProperty), "Response should have property " + countProperty + " for request: '" + request.toString() + "'");
                    } else {
                        Assert.assertFalse(response.has(countProperty), "Response should not have property " + countProperty + " for request: '" + request.toString() + "'");
                    }
                }

                long expectedCount = findCountForRequest(request, entityCounts);
                if (response.has(countProperty) && expectedCount != -1) {
                    long foundCount = response.getLong(countProperty);
                    Assert.assertEquals(foundCount, expectedCount, "Incorrect count for collection of " + request.getEntityType() + " for request: '" + request.toString() + "'");
                }
                Long top = expandQuery.getTop();
                if (top != null && expectedCount != -1) {
                    int foundNumber = response.getJSONArray("value").length();
                    long skip = expandQuery.getSkip() == null ? 0 : expandQuery.getSkip();

                    long expectedNumber = Math.min(expectedCount - skip, top);
                    if (foundNumber != expectedNumber) {
                        Assert.fail("Requested " + top + " of " + expectedCount + ", expected " + expectedNumber + " with skip of " + skip + " but received " + foundNumber + " for request: '" + request.toString() + "'");
                    }

                    if (foundNumber + skip < expectedCount) {
                        // should have nextLink
                        String nextLinkProperty = "@iot.nextLink";
                        Assert.assertTrue(response.has(nextLinkProperty), "Entity should have " + nextLinkProperty + " for request: '" + request.toString() + "'");
                    }

                }

            } else {
                checkEntity(response, request, entityCounts);
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
     * @param entityCounts The object with the expected entity counts.
     * @throws JSONException if there is a problem with the json.
     */
    public static void checkCollection(JSONArray collection, Expand expand, EntityCounts entityCounts) throws JSONException {
        // Check entities
        for (int i = 0; i < collection.length(); i++) {
            checkEntity(collection.getJSONObject(i), expand, entityCounts);
        }
        // todo: check orderby
        // todo: check filter
    }

    /**
     * Check the given entity from a response against the given expand.
     *
     * @param entity The entity to check.
     * @param expand The expand that led to the entity.
     * @param entityCounts The object with the expected entity counts.
     * @throws JSONException if there is a problem with the json.
     */
    public static void checkEntity(JSONObject entity, Expand expand, EntityCounts entityCounts) throws JSONException {
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

        // Entity id in case we need to check counts.
        long entityId = entity.optLong("@iot.id", -1);

        // Check expand
        List<String> relations = new ArrayList<>(entityType.getRelations());
        for (Expand subExpand : query.getExpand()) {
            PathElement path = subExpand.getPath().get(0);
            String propertyName = path.getPropertyName();
            if (!entity.has(propertyName)) {
                Assert.fail("Entity should have expanded " + propertyName + " for request: '" + expand.toString() + "'");
            }

            // Check the expanded items
            if (subExpand.isCollection()) {
                checkCollection(entity.getJSONArray(propertyName), subExpand, entityCounts);
            } else {
                checkEntity(entity.getJSONObject(propertyName), subExpand, entityCounts);
            }
            relations.remove(propertyName);

            // For expanded collections, check count, top, skip
            if (subExpand.isCollection()) {
                // Check count
                Query expandQuery = subExpand.getQuery();
                Boolean count = expandQuery.getCount();
                String countProperty = propertyName + "@iot.count";
                boolean hasCountProperty = entity.has(countProperty);
                if (count != null) {
                    if (count) {
                        Assert.assertTrue(hasCountProperty, "Entity should have property " + countProperty + " for request: '" + expand.toString() + "'");
                    } else {
                        Assert.assertFalse(hasCountProperty, "Entity should not have property " + countProperty + " for request: '" + expand.toString() + "'");
                    }
                }

                long expectedCount = entityCounts.getCount(entityType, entityId, EntityType.getForRelation(propertyName));
                if (hasCountProperty && expectedCount != -1) {
                    long foundCount = entity.getLong(countProperty);
                    Assert.assertEquals(foundCount, expectedCount, "Found incorrect count for " + countProperty);
                }

                Long top = expandQuery.getTop();
                if (top != null && expectedCount != -1) {
                    int foundNumber = entity.getJSONArray(propertyName).length();
                    long skip = expandQuery.getSkip() == null ? 0 : expandQuery.getSkip();

                    long expectedNumber = Math.min(expectedCount - skip, top);
                    if (foundNumber != expectedNumber) {
                        Assert.fail("Requested " + top + " of " + expectedCount + ", expected " + expectedNumber + " with skip of " + skip + " but received " + foundNumber);
                    }

                    if (foundNumber + skip < expectedCount) {
                        // should have nextLink
                        String nextLinkProperty = propertyName + "@iot.nextLink";
                        Assert.assertTrue(entity.has(nextLinkProperty), "Entity should have " + nextLinkProperty + " for expand " + subExpand.toString());
                    }

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

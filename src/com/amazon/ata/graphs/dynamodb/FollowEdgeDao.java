package com.amazon.ata.graphs.dynamodb;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBQueryExpression;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedList;
import com.amazonaws.services.dynamodbv2.datamodeling.PaginatedQueryList;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * Provides access to FollowEdge items.
 */
public class FollowEdgeDao {
    private DynamoDBMapper mapper;

    /**
     * Creates a FollowEdgeDao with the given DynamoDBMapper.
     * @param mapper The DynamoDBMapper
     */
    @Inject
    public FollowEdgeDao(DynamoDBMapper mapper) {
        this.mapper = mapper;
    }

    /**
     * Retrieves a list of follows from the given username, if one exists.
     * @param username The username to look for
     * @return A list of all follows for the given user
     */
    //  USERNAME that follow any other user (FollowEdge is from USERNAME to FOLLOWED_USERNAME)
    //  will be USERNAME added to the FollowEdge table with the FOLLOWED_USERNAME
    public PaginatedQueryList<FollowEdge> getAllFollows(String username) {
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Username cant be null or Empty");
        }
        // retrieve the username through DyanamoDB which have the relationship
        // between USERNAME and FOLLOWED_USERNAME fromUsername at the FollowedEdge
        /*DynamoDBQueryExpression<FollowEdge> queryExpression = new DynamoDBQueryExpression<FollowEdge>()
               .withKeyConditionExpression("USERNAME = :username")
               .withExpressionAttributeValues(new HashMap<String, AttributeValue>() {{
                    put(":username", new AttributeValue(username));
                }});

        return mapper.query(FollowEdge.class, queryExpression);*/

        DynamoDBQueryExpression<FollowEdge> queryExpression = new DynamoDBQueryExpression<>();
        //A new DynamoDBQueryExpression is created for the FollowEdge class.
        FollowEdge parameter = new FollowEdge(username, null);
        //A FollowEdge object is created with the given username as the hash key (likely the
        // fromUsername in the table).
        queryExpression.withHashKeyValues(parameter);
        //The query expression is set up to use this FollowEdge object as the hash key values for the query.

        return mapper.query(FollowEdge.class, queryExpression);
        //The query is executed using the DynamoDBMapper, which will return all
        // FollowEdge entries where the fromUsername matches the provided username.




    }// We use from username as the key to find the followers of a user
    //username as the first parameter because at the consrtuctor FollowEdge the
    // first parameter is fromUsername and the second parameter is toUsername
    //we use here the first instance parameter username as the key(fromUsername

    /**
     * Retrieves a list of followers for the given username, if one exists.
     * @param username The username to scope followers to
     * @return A list of all followers for the given user
     */
     
    public PaginatedQueryList<FollowEdge> getAllFollowers(String username) {
        //retrieve a list of followers for a given username
        //this method takes a username as input and returns a PaginatedQueryList of FollowEdge objects
        if (username == null || username.isEmpty()) {
            throw new IllegalArgumentException("Username cant be null or Empty");
        }
        DynamoDBQueryExpression<FollowEdge> queryExpression = new DynamoDBQueryExpression<>();
        //A new DynamoDBQueryExpression is created for the FollowEdge class.
        Map<String, AttributeValue> attributeValueMap = new HashMap<>();
        attributeValueMap.put(":username", new AttributeValue().withS(username));

        queryExpression.withKeyConditionExpression("toUsername = :username")
                .withExpressionAttributeValues(attributeValueMap);
        //The query expression is set up to use this FollowEdge object as the hash key values
        // for the query.
        return mapper.query(FollowEdge.class, queryExpression);
        //The query is executed using the DynamoDBMapper, which will return all
        // FollowEdge entries where the toUsername matches the provided username.

    }

    // We use toUsername as the key to find the followers of a user
    //username as the second parameter because at the consrtuctor FollowEdge the
    // first parameter is fromUsername and the second parameter is toUsername
    //we use here the second instance parameter username as the key(toUsername)


    /**
     * Saves new follow.
     * @param fromUsername The Member that is following
     * @param toUsername The Member that is followed
     * @return The FollowEdge that was created
     */
    public FollowEdge createFollowEdge(String fromUsername, String toUsername) {
        if (null == fromUsername || null == toUsername) {
            throw new IllegalArgumentException("One of the passed in usernames was null: " + fromUsername +
                    " was trying to follow " + toUsername);
        }

        FollowEdge edge = new FollowEdge(fromUsername, toUsername);
        mapper.save(edge);
        return edge;
    }

}

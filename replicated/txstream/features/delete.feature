Feature: replicating delete operation

    Scenario: deleting an existing map
        Given empty source and destination database
        And a replicated map with path "foo"
        When I replicate deleting path "foo"
        Then the path "foo" should not exist in the destination database
    
    Scenario: deleting not existing map
        Given empty source and destination database
        When I try to delete a map "foo"
        Then NotExisting error should be returned in the original transaction


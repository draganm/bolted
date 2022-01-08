Feature: Replicating Transaction

    Scenario: Replicating PUT
        Given an empty primary server
        And a replica connected to the primary
        When I execute putting of "foo" on the path "bar" on the replica
        Then the primary server should have value "foo" on the path "bar"
        And the replica should have value "foo" on the path "bar"

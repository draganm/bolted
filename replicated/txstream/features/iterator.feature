Feature: asserting iteration results when replicating

    Scenario: Successful iteration of empty root
        Given empty source and destination database
        When I replicate iteration over the root
        Then the replication should succeed

    Scenario: Successful iteration of root with one element
        Given empty source and destination database
        And I replicate putting "bar" into path "foo"
        When I replicate iteration over the root
        Then the replication should succeed

    Scenario: Failed iteration of root when destination has changed
        Given empty source and destination database
        And I replicate putting "bar" into path "foo"
        And the value of "foo" has changed to "baz" in the destination database
        When I replicate iteration over the root
        Then Stale error should be returned on replication

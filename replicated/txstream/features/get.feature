Feature: getting replicated data

    Scenario: Replicating get
        Given empty source and destination database
        And I replicate putting "bar" into path "foo"
        When I try to replicate getting the value "foo"
        Then the replication should succeed

    Scenario: Getting stale short data from root
        Given empty source and destination database
        And I replicate putting "bar" into path "foo"
        And the value of "foo" has changed to "baz" in the destination database
        When I try to replicate getting the value "foo"
        Then Stale error should be returned on replication

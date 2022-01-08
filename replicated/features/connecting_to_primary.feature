Feature: connecting to the primary server

    Scenario: connecting to an empty database
        Given an empty primary server
        When I connect to the primary server
        Then the connecting should succeed

    Scenario: connecting to an primary containing data
        Given an running primary server
        And value of "foo" being set ot "bar" on the primary server
        When I connect to the primary server
        Then the connecting should succeed
        And the value of "foo" should be "bar" on the replica

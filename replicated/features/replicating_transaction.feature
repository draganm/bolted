Feature: Replicating Transaction

    Scenario: Replicating PUT back to the original replica
        Given an running primary server
        And value of "foo" being set ot "bar" on the primary server
        When I connect to the primary server
        Then the connecting should succeed
        And the value of "foo" should be "bar" on the replica

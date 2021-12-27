Feature: asserting result of size on the replica

    Scenario: Getting stale size of the replicated database fails
        Given empty source and destination database
        And the I put "test" into path "baz" in the destination database
        When I try replicating geting the size of root
        Then Stale error should be returned on replication

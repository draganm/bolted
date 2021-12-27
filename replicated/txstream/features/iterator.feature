Feature: asserting iteration results when replicating

    Scenario: Getting size of the replicated
        Given empty source and destination database
        When I replicate iteration over the root
        Then the replication should succeed

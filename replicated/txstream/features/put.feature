Feature: put

    Scenario: putting data into root
        Given empty source and destination database
        When I replicate putting "bar" into path "foo"
        Then the destination database should have a value with path "foo"

    Scenario: putting data into root when map already exists
        Given empty source and destination database
        And a replicated map with path "foo"
        When I try putting "bar" into path "foo"
        Then Conflict error should be returned in the original transaction

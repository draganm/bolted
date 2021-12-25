Feature: replicating create map

    Scenario: creating map in root
        Given empty source and destination database
        When I replicate creating a new map with path "foo"
        Then the destination database should have a map with path "foo"

    Scenario: creating nested maps
        Given empty source and destination database
        When I replicate creating a new map with path "foo"
        And I replicate creating a new map with path "foo/bar"
        Then the destination database should have a map with path "foo/bar"

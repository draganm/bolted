Feature: connecting to the primary server

    Scenario: connecting to an empty database
        Given an empty primary server
        When I connect to the primary server
        Then the connecting should succeed


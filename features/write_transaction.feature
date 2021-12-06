Feature: write transaction

Background: 
    Given the database is open

    Scenario: creating a map in the root
        When I create a map "abc"
        Then the map "abc" should exist
        And the map "abc" should be empty
        And the root should have 1 element

    Scenario: deleting a map in the root
        Given I have created a map "abc"
        When I delete the map "abc"
        And the map "abc" should not exist
        And the root should have 0 elements

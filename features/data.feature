Feature: handling of data

Background: 
    Given the database is open

    Scenario: putting data in the root
        When I put "test" data under "abc" in the root
        Then the data "abc" should exist
        And the context of the data "abc" should be "test"
        And the root should have 1 element

    Scenario: deleting data in the root
        Given there is data with name "abc" in the root
        When I delete data "abc" from the root
        Then the data "abc" should not exist
        And the root should have 0 elements

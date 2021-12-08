Feature: iteration

Scenario: iterating over all entries
    Given the database is open
    And there are 5 maps and 5 data entries in the root
    When I iterate over all entries
    Then I should retrieve entries in order
    
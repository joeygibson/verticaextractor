Feature: Test basic functionality
  Background:
    Given I have waited no more than 60 seconds for Vertica to be ready
    And there are no tables

  Scenario: First test
    And I start an instance of the service arguments:
    """
    """
    And I have waited no more than 20 seconds for it to startup

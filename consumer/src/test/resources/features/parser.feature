# new feature
# Tags: optional
    
Feature: Test du Parser de Data Table (csv + schema)
    
Scenario: récuperer la ligne telle quelle
    Given la table suivante :
    | name:String   |  age:Integer  |
    | A             | 18            |

    When lors ce que j'appelle le parser

    Then j'aurai le name suivant :
    | A |

    And j'aurai l'age suivant :
    | 18 |


Scenario: récuperer le schema
    Given la table suivante :
    | name:String   |  age:Integer      |
    | A             | 18                |
    | B             | 20                |

    When lors ce que j'appelle le parser :

    Then j'aurai le schema de name suivant :
    | String |

    And j'aurai le schema de age suivant :
    | Integer |



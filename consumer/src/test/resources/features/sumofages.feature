Feature: Calcul de la somme des ages

Scenario:Calcul de la somme des ages
    Given la dataframe suivante :
    | name:String   |  age:Integer  |
    | A             | 18            |
    | B             | 20            |
    | C             | 28            |
    | D             | 38            |
    | E             | 48            |

    When lors ce que je calcule la somme des ages

    Then j'aurai la somme suivante :
    | 152 |

package com.test.spark.wiki.extracts

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Double)
package com.test.spark.wiki.extracts.bddprocessing.runner

import cucumber.api.CucumberOptions
import cucumber.api.junit.Cucumber
import org.junit.runner.RunWith

@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("src/test/resources/features/"),
  glue = Array("classpath:com.test.spark.wiki.extracts.bddprocessing.steps")
)
class RunBDDTest
package com.qaworks.scala

import org.junit.runner.RunWith
import cucumber.api.junit.Cucumber
import cucumber.api.CucumberOptions




@RunWith(classOf[Cucumber])
@CucumberOptions(
  features = Array("src/test/resources/features"),
  glue = Array("com/qaworks/scala"),
  plugin = Array("pretty", "html:target/cucumber-html-report"),
  tags = Array("@date_tab")
)
class RunCukesTest {
  
  
  
}

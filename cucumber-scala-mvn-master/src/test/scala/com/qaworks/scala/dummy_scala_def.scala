//package com.qaworks.scala
//import cucumber.api.scala.{EN, ScalaDsl}
//import org.scalatest.Matchers
//import java.util.concurrent.TimeUnit;
// 
//import org.openqa.selenium.WebDriver;
// 
//import org.openqa.selenium.chrome.ChromeDriver;
//
//class dummy_scala_def extends ScalaDsl with EN with Matchers{
//  
//  
//  
//  var url: String = "https://www.google.com"
//    val driver:WebDriver = new ChromeDriver() 
//
//  
//Given("""^user opens the webbrowser$"""){ () =>
//  
// 
//  
// 
//}
//  
//When("""^user is in FMS page$"""){ () =>
//  
//    driver.get("https://www.google.com/");
//}
//  
//Then("""^validate the FMS url$"""){ () =>
// 
//  driver.getCurrentUrl.equals(url)
//  
//}
//  
//}
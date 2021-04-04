plugins {
    java
    scala
    application
}

group = "org.example"
version = "1.0-SNAPSHOT"

application {
    mainClass.set("sandbox.scala.Main")
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.13.5")
    implementation("org.elasticmq:elasticmq-rest-sqs_2.13:1.1.0")
    implementation("software.amazon.awssdk:sqs:2.16.20")
    implementation("software.amazon.awssdk:netty-nio-client:2.16.20")

    implementation("org.rogach:scallop_2.13:4.0.2")
    implementation("com.typesafe.scala-logging:scala-logging_2.13:3.9.3")
    implementation("ch.qos.logback:logback-classic:1.2.3")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
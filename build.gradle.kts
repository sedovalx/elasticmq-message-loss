plugins {
    java
    scala
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.13.5")
    implementation("org.elasticmq:elasticmq-rest-sqs_2.13:1.1.0")
    implementation("software.amazon.awssdk:sqs:2.16.20")
    implementation("software.amazon.awssdk:netty-nio-client:2.16.20")
    runtimeOnly("ch.qos.logback:logback-classic:1.2.3")
}

tasks.getByName<Test>("test") {
    useJUnitPlatform()
}
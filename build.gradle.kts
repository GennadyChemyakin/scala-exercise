group = "spark-hw"
version = "1.0-SNAPSHOT"


plugins {
    scala
    java
}


repositories {
    mavenCentral()
}

dependencies {
    implementation("org.scala-lang:scala-library:2.12.8")
    testImplementation("org.scalatest:scalatest_2.11:3.0.0")
    testImplementation("junit:junit:4.12")

    compile("org.apache.spark:spark-core_2.12:2.4.3")
    compile("org.apache.spark:spark-sql_2.12:2.4.3")

}
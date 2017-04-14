import scala.util.Properties.isJavaAtLeast

object Versions {
  lazy val netty = "4.0.42.Final"
  lazy val jobServer = "-SNAPSHOT"
  lazy val spark = "2.0.0"
  lazy val typesafeConfig = if (isJavaAtLeast("1.8")) "1.3.0" else "1.2.1"
}

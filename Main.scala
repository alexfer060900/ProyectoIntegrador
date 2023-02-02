import com.github.tototoshi.csv._
import com.sun.tools.javac.resources.CompilerProperties.Fragments.Location
import play.api.libs.json.Format.GenericFormat
import play.api.libs.json.OFormat.oFormatFromReadsAndOWrites
import play.api.libs.json.{JsArray, JsBoolean, JsError, JsNull, JsNumber, JsObject, JsPath, JsString, JsSuccess, JsValue, Json, Reads, Writes}
import com.cibo.evilplot._
import com.cibo.evilplot.plot.{BarChart, PieChart}
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultElements, DefaultTheme, defaultTheme}
import requests.Response

import scala.language.postfixOps


import java.io.File
import java.lang
import scala.{None, Option, Some}
import scala.util.{Failure, Success, Try}
import scala.util.matching.Regex


object Main extends App {
  val reader = CSVReader.open(new File("C:\\Users\\alexa\\Desktop\\UNIVERSIDAD\\Datasets-60922f96f3bd82568eb2bf398c404afe218f4fa0/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()


  /*def double(s: String): Try[Double] = {
    Try {
      s.toDouble
    }
  }

  def int(s: String): Try[Int] = {
    Try {
      s.toInt
    }
  }

  val presupuesto = data
    .flatMap(elem => elem.get("budget")).map(k => int(k)).filter(k => k.isSuccess).map(_.get)
  val presupuesto2 = presupuesto.filter(k => k > 0)
  val budgetMax = presupuesto.max
  val budgetMin = presupuesto2.min
  val budgetAvg1 = presupuesto.sum / presupuesto.size
  val budgetAvg2 = presupuesto2.sum / presupuesto2.size
  println("El presupuesto maximo es: " + budgetMax + "\nEl presupuesto minimo (sin considerar el 0) es: "
    + budgetMin + "\nEl promedio de los presupuestos (teniendo en cuenta el 0) es: "
    + budgetAvg1 + "\nEl promedio de los presupuestos (sin considerar el 0) es: " + budgetAvg2)
  val id = data
    .flatMap(elem => elem.get("id")).map(k => int(k)).filter(k => k.isSuccess).map(_.get).filter(k => k > 0)
  val idMax = id.max
  val idMin = id.min
  val idAvg = id.sum / id.size
  println("El numero de identificador maximo es: " + idMax + "\nEl numero de identificador minimo es: " + idMin +
    "\nEl promedio de los identificadores es: " + idAvg)
  val popular = data
    .flatMap(elem => elem.get("popularity")).map(k => double(k)).filter(k => k.isSuccess).map(_.get).filter(k => k > 0)
  val popularMax = popular.max
  val popularMin = popular.min
  val popularAvg = popular.sum / popular.size
  println("El indice de popularidad maximo es: " + popularMax + "\nEl indice de popularidad minimo es: " + popularMin +
    "\nEl promedio de los indices de popularidad es: " + popularAvg)
  val revenue = data
    .flatMap(elem => elem.get("revenue")).map(k => double(k)).filter(k => k.isSuccess).map(_.get).filter(k => k > 0)
  val revenueMax = revenue.max
  val revenueMin = revenue.min
  val revenueAvg = revenue.sum / revenue.size
  println("La maxima cantidad recolectada por una pelicula es: " + revenueMax +
    "\nLa minima cantidad recolectada por una pelicula es: " + revenueMin +
    "\nEl promedio de las cantidades recolectadas por todas las peliculas es: " + revenueAvg)

  val runtime = data
    .flatMap(elem => elem.get("runtime")).map(k => double(k)).filter(k => k.isSuccess).map(_.get).filter(k => k > 0)
  val runtimeMax = runtime.max
  val runtimeMin = runtime.min
  val runtimeAvg = runtime.sum / runtime.size
  println("La maxima duracion que tiene una pelicula es: " + runtimeMax + "\nLa minima duracion que tiene una " +
    "pelicula es: " + runtimeMin + "\nEl promedio de las duraciones que tienen todas las peliculas es: "
    + runtimeAvg)

  val voteAvg = data
    .flatMap(elem => elem.get("vote_average")).map(k => double(k)).filter(k => k.isSuccess).map(_.get).filter(k => k > 0)
  val voteAvgMax = voteAvg.max
  val voteAvgMin = voteAvg.min
  val voteAvgAvg = voteAvg.sum / voteAvg.size
  println("La maxima puntuacion que tiene una pelicula es: " + voteAvgMax + "\nLa minima puntuacion que tiene una " +
    "pelicula es: " + voteAvgMin + "\nEl promedio de las puntuaciones que tienen todas las peliculas es: "
    + voteAvgAvg)

  val voteCount = data
    .flatMap(elem => elem.get("vote_count")).map(k => int(k)).filter(k => k.isSuccess).map(_.get).filter(k => k > 0)
  val voteCountMax = voteCount.max
  val voteCountMin = voteCount.min
  val voteCountAvg = voteCount.sum / voteCount.size
  println("La maxima cantidad de votantes que tiene una pelicula es: " + voteCountMax +
    "\nLa minima cantidad de votantes que tiene una pelicula es: " + voteCountMin +
    "\nEl promedio de todos los numeros de votantes que tienen todas las peliculas es: " + voteCountAvg)

  val director = data
    .flatMap(elem => elem.get("director")).filter(k => k != "").groupBy(k => k).maxBy(k => k._2.size)._1
  println("El director que mas se repite en el dataset es: " + director)

  val status = data
    .flatMap(elem => elem.get("status")).filter(k => k != "").groupBy(k => k).maxBy(k => k._2.size)._1
  println("El status de la pelicula que mas se repite en el dataset es: " + status)

  val language = data
    .flatMap(elem => elem.get("original_language")).filter(k => k != "").groupBy(k => k).maxBy(k => k._2.size)._1
  println("El idioma original que mas se repite en el dataset es: " + language)

  val genres = data
    .flatMap(elem => elem.get("genres")).filter(k => k != "").groupBy(k => k).maxBy(k => k._2.size)._1
  println("El genero de peliculas que mas se repite en el dataset es: " + genres)

  val json = data.flatMap(elem => elem.get("production_companies").filter(k => k != "[]")).toString()
  val json1 = Json.toJson(json)
  val json2 = Json.stringify(json1)
  val json3 = Json.parse(json2)



  val name = (json3 \"name").asOpt[String]


  println(name)


  def replacePatterns(original : String) : String = {
    var txtOr = original
    val pattern: Regex = "(\\s\"(.*?)\",)".r
    val pattern2: Regex = "([a-z]\\s\"(.*?)\"\\s*[A-Z])".r
    val pattern3: Regex = "(:\\s'\"(.*?)',)".r



    txtOr
  }



  val crew = data
    .map(row => row("crew"))
    .map(replacePatterns)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Try(Json.parse(text)))
    .filter(_.isSuccess)
    .size

  println(crew)

  def replacePattern(original: String): String = {
    var txtOr = original
    val pattern: Regex = "(\\s\"(.*?)\",)".r
    for (m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("'", "-u0027")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }

  def replacePattern2(original: String): String = {
    var txtOr = original
    val pattern: Regex = "([a-z]\\s\"(.*?)\"\\s*[A-Z])".r
    for (m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }

  def replacePattern3(original: String): String = {
    var txtOr = original
    val pattern: Regex = "(:\\s'\"(.*?)',)".r
    for (m <- pattern.findAllIn(original)) {
      val textOriginal = m
      val replacementText = m.replace("\"", "-u0022")
      txtOr = txtOr.replace(textOriginal, replacementText)
    }
    txtOr
  }


  val crew2 = data
    .map(row => row("crew"))
    .map(replacePattern2)
    .map(replacePattern)
    .map(replacePattern3)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Json.parse(text))


  println(crew2)

  val departments = data
    .filter(k => k("title") == "Avatar")
    .map(row => row("crew"))
    .map(replacePattern2)
    .map(replacePattern)
    .map(replacePattern3)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Json.parse(text))
    .flatMap(jsonData => jsonData \\ "department")
    .map(jsValue => jsValue.as[String])

  println(departments)

  val jobs = data
    .filter(k => k("title") == "Avatar")
    .map(row => row("crew"))
    .map(replacePattern2)
    .map(replacePattern)
    .map(replacePattern3)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Json.parse(text))
    .flatMap(jsonData => jsonData \\ "job")
    .map(jsValue => jsValue.as[String])

  println(jobs)

  val crewDepartments = crew2
    .flatMap(jsonData => jsonData \\ "department")
    .map(jsValue => jsValue.as[String])
    .toSeq

  println(crewDepartments)

  val crewJobs = crew2
    .flatMap(jsonData => jsonData \\ "job")
    .map(jsValue => jsValue.as[String])
    .toSeq

  println(crewJobs)

  val crewId = crew2
    .flatMap(jsonData => jsonData \\ "id")
    .map(jsValue => jsValue.as[Int])
    .groupBy(identity)
    .map{case (keyword, lista) => (keyword, lista.size)}
    .toList

  println(crewId)

  val popularDepartments = crew2
    .flatMap(jsonData => jsonData \\ "department")
    .map(jsValue => jsValue.as[String])

  val departmentValues = popularDepartments.take(5).map(_._2).map(_.toDouble)
  val departmentNames = popularDepartments.take(5).map(_._1)

  BarChart(departmentValues)
    .title("Departamentos mas populares")
    .xAxis(departmentNames)
    .yAxis()
    .frame()
    .yLabel("Popularidad")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\alexa\\Desktop\\UNIVERSIDAD\\histograma.png"))


  val genders = crew2
    .flatMap(jsonData => jsonData \\ "gender")
    .map(jsValue => jsValue.as[Int])
    .groupBy(identity)
    .map { case (keyword, lista) => (keyword.toString, lista.size.toDouble) }
    .toList


  PieChart(genders)
    .rightLegend()
    .render()
    .write(new File("C:\\Users\\alexa\\Desktop\\UNIVERSIDAD\\piechart.png"))



  def actorsNames(dataRaw: String): Option[String] = {
    val response: Response = requests
      .post("http://api.meaningcloud.com/topics-2.0",
        data = Map("key" -> "5e8d32277bab3b0b9028eeee11738d2f",
          "lang" -> "en",
          "txt" -> dataRaw,
          "tt" -> "e"),
        headers = Map("content-type" -> "application/x-www-form-urlencoded"))
    Thread.sleep(1000)
    if (response.statusCode == 200) {
      Option(response.text)
    } else
      Option.empty
  }

  val cast = data
    .map(row => row("cast"))
    .filter(_.nonEmpty)
    .map(StringContext.processEscapes)
    .take(100) //Use un número limitado para hacer sus pruebas, pero, al final debe analizar todos los datos.
    .map(actorsNames)
    .map(json => Try(Json.parse(json.get)))
    .filter(_.isSuccess)
    .map(_.get)
    .flatMap(json => json("entity_list").as[JsArray].value)
    .map(_("form"))
    .map(data => data.as[String])

  println(cast)

  sql"""
  CREATE TABLE CAST (
  NAME varchar(64) DEFAULT NULL
  )
  """.execute.apply()

  cast.foreach  {
    row => sql"insert into cast (name) values (${row})"
  .update.apply()
  }

  val entities = sql"SELECT * FROM CAST WHERE (NAME = 'Will Smith')".map(_.toMap()).list.apply()

  println(entities)


  sql"DROP TABLE CAST".update.apply()

  def actorsNames(dataRaw: String): Option[String] = {
    val response: Response = requests
      .post("http://api.meaningcloud.com/topics-2.0",
        data = Map("key" -> "su API_KEY debe ir aquí",
          "lang" -> "en",
          "txt" -> dataRaw,
          "tt" -> "e"),
        headers = Map("content-type" -> "application/x-www-form-urlencoded"))
    Thread.sleep(1000)
    if (response.statusCode == 200) {
      Option(response.text)
    } else
      Option.empty
  }
  def transform (jsValue: JsValue) =
    jsValue("entity_list").as[JsArray].value
      .map(_("form"))
      .map(_.as[String])
      .toList

  val castId = data
    .map( row => (row("id"), row("data")))
    .filter(_._2.nonEmpty)
    .map(tuple2 => (tuple2._1, StringContext.processEscapes(tuple2._2)))
    .take(10)
    .map{t2 => (t2._1, actorsNames(t2._2))}
    .map{t2 => (t2._1, Try(Json.parse(t2._2.get)))}
    .filter(_._2.isSuccess)
    .map(t2 => (t2._1, t2._2.get))
    .map(t2 => (t2._1, transform(t2._2)))
    .flatMap(t2 => t2._2.map(name => (t2._1, name)))
    .map(_.productIterator.toList)
    .distinct

  val f = new File ("C:\\Users\\alexa\\Desktop\\UNIVERSIDAD\\movie-cast.csv")
  val writer = CSVWriter.open(f)
  writer.writeAll(castId)
  writer.close()

  case class Movie(id: String,
                   originalLanguage: String,
                   originalTitle: String,
                   budget : Long,
                   popularity: Double,
                   runtime: Double,
                   revenue: Long)

  val movieData = data
    .map(row => Movie(
      row("id"),
      row("original_language"),
      row("original_title"),
      row("budget").toLong,
      row("popularity").toDouble,
      row("runtime") match {
        case valueofRT if valueofRT.trim.isEmpty => 0.0
        case valueofRT => valueofRT.toDouble
      },
      row("revenue").toLong))

  val SQL_INSERT_PATTERN =
    """INSERT INTO MOVIE(`RAW_ID`, `ORIGINAL_TITLE`, `BUDGET`, `POPULARITY`, `RUNTIME`, `REVENUE`, `ORIGINAL_LANGUAGE`)
    VALUES
    ('%s', '%s', %d, %f, %f, %d, '%s');
    """.stripMargin

  def escapeMySql(text: String): String = text
    .replaceAll("\\\\", "\\\\\\\\")
    .replaceAll("\b", "\\\\b")
    .replaceAll("\n", "\\\\n")
    .replaceAll("\r", "\\\\r")
    .replaceAll("\t", "\\\\t")
    .replaceAll("\\x1A", "\\\\Z")
    .replaceAll("\\x00", "\\\\0")
    .replaceAll("'", "\\\\'")
    .replaceAll("\"", "\\\\\"")

  val scriptData = movieData
    .map(movie => SQL_INSERT_PATTERN.formatLocal(java.util.Locale.US))



   */



}

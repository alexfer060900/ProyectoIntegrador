import com.cibo.evilplot.demo.DemoPlots.theme
import com.cibo.evilplot.plot.BarChart
import com.cibo.evilplot.plot.aesthetics.DefaultTheme.{DefaultElements, DefaultTheme}
import com.github.tototoshi.csv.CSVReader
import play.api.libs.json.Json

import java.io.File

object columNoNumericas extends App {
  val reader = CSVReader.open(new File("C:\\Users\\alexa\\Desktop\\UNIVERSIDAD\\Datasets-60922f96f3bd82568eb2bf398c404afe218f4fa0/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  val director = data
    .flatMap(elem => elem.get("director"))
    .filter(k => k != "")
    .groupBy(k => k)
    .maxBy(k => k._2.size)
    ._1

  println("El director que mas se repite en el dataset es: " + director)

  val status = data
    .flatMap(elem => elem.get("status"))
    .filter(k => k != "")
    .groupBy(k => k)
    .maxBy(k => k._2.size)
    ._1

  println("El status de la pelicula que mas se repite en el dataset es: " + status)

  val language = data
    .flatMap(elem => elem.get("original_language"))
    .filter(k => k != "")
    .groupBy(k => k)
    .maxBy(k => k._2.size)
    ._1

  println("El idioma original que mas se repite en el dataset es: " + language)

  val productionCompanies = data
    .flatMap(elem => elem.get("production_companies"))
    .map(k => Json.parse(k))
    .flatMap(jsonData => jsonData \\ "name")
    .map(js => js.as[String])
    .groupBy(identity)
    .map {case (key, list) => (key, list.size)}
    .toList
    .sortBy(_._2)
    .reverse

  val companiesValues = productionCompanies.take(10).map(_._2).map(_.toDouble)
  val companiesNames = productionCompanies.take(10).map(_._1)

  implicit val theme = DefaultTheme.copy(
    elements = DefaultElements.copy(categoricalXAxisLabelOrientation = 45)
  )

  BarChart(companiesValues)
    .title("Compañias mas populares")
    .xAxis(companiesNames)
    .yAxis()
    .frame()
    .yLabel("Popularidad")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\alexa\\Desktop\\barrasCompañias.png"))


  val productionCountries = data
    .flatMap(elem => elem.get("production_countries"))
    .map(k => Json.parse(k))
    .flatMap(jsonData => jsonData \\ "name")
    .map(js => js.as[String])
    .groupBy(identity)
    .map { case (key, list) => (key, list.size) }
    .toList
    .sortBy(_._2)
    .reverse

  val countriesValues = productionCountries.take(10).map(_._2).map(_.toDouble)
  val countriesNames = productionCountries.take(10).map(_._1)

  BarChart(countriesValues)
    .title("Paises mas populares")
    .xAxis(countriesNames)
    .yAxis()
    .frame()
    .yLabel("Popularidad")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\alexa\\Desktop\\barrasPaises.png"))

  val spokenLang = data
    .flatMap(elem => elem.get("spoken_languages"))
    .map(k => Json.parse(k))
    .flatMap(jsonData => jsonData \\ "name")
    .map(js => js.as[String])
    .groupBy(identity)
    .map { case (key, list) => (key, list.size) }
    .toList
    .sortBy(_._2)
    .reverse

  val languageValues = spokenLang.take(10).map(_._2).map(_.toDouble)
  val languageNames = spokenLang.take(10).map(_._1)


  BarChart(languageValues)
    .title("Idiomas mas populares")
    .xAxis(languageNames)
    .yAxis()
    .frame()
    .yLabel("Popularidad")
    .bottomLegend()
    .render()
    .write(new File("C:\\Users\\alexa\\Desktop\\barrasIdiomas.png"))


}

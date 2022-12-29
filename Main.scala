import com.github.tototoshi.csv._

import java.io.File
import java.lang
import scala.util.{Failure, Success, Try}

object Main extends App {
  val reader = CSVReader.open(new File("C:\\Users\\alexa\\Desktop\\UNIVERSIDAD\\Datasets-60922f96f3bd82568eb2bf398c404afe218f4fa0/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  def double(s: String): Try[Double] = {
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

  val date = data
    .flatMap(elem => elem.get("release_date")).map(String.valueOf(k => k.split("-")[1]))//.filter(k => k != "").groupBy(k => k).maxBy(k => k._2.size)._1
  println("El año que mas lanzamiento de peliculas hubo segun el dataset es: " + date)


}

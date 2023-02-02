import columNumericas.max
import com.github.tototoshi.csv.CSVReader

import java.io.File
import scala.util.Try

object columNumericas extends App {
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

  def max(x: List[Double]) =
    x.max

  def min(x: List[Double]) =
    x.min

  def average (x: List[Double]) =
    x.sum / x.size


  val presupuesto = data
    .flatMap(elem => elem.get("budget"))
    .map(k => int(k))
    .filter(k => k.isSuccess)
    .map(_.get)
    .filter(k => k > 0)
    .map(_.toDouble)

  val budgetMax = max(presupuesto)
  val budgetMin = min(presupuesto)
  val budgetAverage = average(presupuesto)

  println("El presupuesto maximo que tuvo una pelicula: " + budgetMax +
    "\nEl presupuesto minimo que tuvo una pelicula es: " + budgetMin +
    "\nEl promedio de los presupuestos que tuvieron todas las peliculas (obviando el 0)  " + budgetAverage)

  val popular = data
    .flatMap(elem => elem.get("popularity"))
    .map(k => double(k))
    .filter(k => k.isSuccess)
    .map(_.get)
    .filter(k => k > 0)

  val popularMax = max(popular)
  val popularMin = min(popular)
  val popularAvg = average(popular)
  println("El indice de popularidad maximo es: " + popularMax +
    "\nEl indice de popularidad minimo es: " + popularMin +
    "\nEl promedio de los indices de popularidad es: " + popularAvg)

  val revenue = data
    .flatMap(elem => elem.get("revenue"))
    .map(k => double(k))
    .filter(k => k.isSuccess)
    .map(_.get)
    .filter(k => k > 0)

  val revenueMax = max(revenue)
  val revenueMin = min(revenue)
  val revenueAvg = average(revenue)
  println("La maxima cantidad recolectada por una pelicula es: " + revenueMax +
    "\nLa minima cantidad recolectada por una pelicula es: " + revenueMin +
    "\nEl promedio de las cantidades recolectadas por todas las peliculas es: " + revenueAvg)

  val runtime = data
    .flatMap(elem => elem.get("runtime"))
    .map(k => double(k))
    .filter(k => k.isSuccess)
    .map(_.get)
    .filter(k => k > 0)

  val runtimeMax = max(runtime)
  val runtimeMin = min(runtime)
  val runtimeAvg = average(runtime)

  println("La maxima duracion que tiene una pelicula es: " + runtimeMax +
    "\nLa minima duracion que tiene una pelicula es: " + runtimeMin +
    "\nEl promedio de las duraciones que tienen todas las peliculas es: " + runtimeAvg)

  val voteAvg = data
    .flatMap(elem => elem.get("vote_average"))
    .map(k => double(k))
    .filter(k => k.isSuccess)
    .map(_.get)
    .filter(k => k > 0)

  val voteAvgMax = max(voteAvg)
  val voteAvgMin = min(voteAvg)
  val voteAvgAvg = average(voteAvg)

  println("La maxima puntuacion que tiene una pelicula es: " + voteAvgMax +
    "\nLa minima puntuacion que tiene una pelicula es: " + voteAvgMin +
    "\nEl promedio de las puntuaciones que tienen todas las peliculas es: " + voteAvgAvg)

  val voteCount = data
    .flatMap(elem => elem.get("vote_count"))
    .map(k => int(k))
    .filter(k => k.isSuccess)
    .map(_.get)
    .filter(k => k > 0)
    .map(_.toDouble)

  val voteCountMax = max(voteCount)
  val voteCountMin = min(voteCount)
  val voteCountAvg = average(voteCount)

  println("La maxima cantidad de votantes que tiene una pelicula es: " + voteCountMax +
    "\nLa minima cantidad de votantes que tiene una pelicula es: " + voteCountMin +
    "\nEl promedio de todos los numeros de votantes que tienen todas las peliculas es: " + voteCountAvg)


}

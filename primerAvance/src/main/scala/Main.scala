import com.github.tototoshi.csv._

import java.io.File
import scala.util.{Failure, Success, Try}

object Main extends App {
  val reader = CSVReader.open(new File("C:\\Users\\alexa\\Desktop\\UNIVERSIDAD\\Datasets-60922f96f3bd82568eb2bf398c404afe218f4fa0/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

  val presupuesto = data
    .flatMap(elem => elem.get("budget")).map(k => k.toInt)
  val presupuesto2 = presupuesto.filter(k => k > 0)
  val budgetMax = presupuesto.max
  val budgetMin = presupuesto2.min
  val budgetAvg1 = presupuesto.sum / presupuesto.size
  val budgetAvg2 = presupuesto2.sum / presupuesto2.size
  println("El presupuesto maximo es: " +budgetMax+ "\nEl presupuesto minimo (sin considerar el 0) es: "
    +budgetMin+ "\nEl promedio de los presupuestos (teniendo en cuenta el 0) es: "
    +budgetAvg1+ "\nEl promedio de los presupuestos (sin considerar el 0) es: " +budgetAvg2)
  val id = data
    .flatMap(elem => elem.get("id")).map(k => k.toInt)
  val idMax = id.max
  val idMin = id.min
  val idAvg = id.sum / id.size
  println("El numero de identificador maximo es: " + idMax + "\nEl numero de identificador minimo es: " + idMin +
    "\nEl promedio de los identificadores es: " + idAvg)
  val popular = data
    .flatMap(elem => elem.get("popularity")).map(k => k.toDouble)
  val popularMax = popular.max
  val popularMin = popular.min
  val popularAvg = popular.sum / popular.size
  println("El indice de popularidad maximo es: " + popularMax + "\nEl indice de popularidad minimo es: " + popularMin +
    "\nEl promedio de los indices de popularidad es: " + popularAvg)
  val revenue = data
    .flatMap(elem => elem.get("revenue")).map(k => k.toDouble)
  val revenueMax = revenue.max
  val revenueMin = revenue.min
  val revenueAvg = revenue.sum / revenue.size
  println("La maxima cantidad recolectada por una pelicula es: " + revenueMax +
    "\nLa minima cantidad recolectada por una pelicula es: " + revenueMin +
    "\nEl promedio de las cantidades recolectadas por todas las peliculas es: " + revenueAvg)

  val runtime = data
    .flatMap(elem => elem.get("runtime")).map(k => k.toDouble)
  val runtimeMax = runtime.max
  val runtimeMin = runtime.min
  val runtimeAvg = runtime.sum / runtime.size
  println("La maxima duracion que tiene una pelicula es: " + runtimeMax + "\nLa minima duracion que tiene una " +
  "pelicula es: " + runtimeMin + "\nEl promedio de las duraciones que tienen todas las peliculas es: "
  + runtimeAvg)

}

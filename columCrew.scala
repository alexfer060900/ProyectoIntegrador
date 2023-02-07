import com.github.tototoshi.csv.CSVReader
import scala.util.matching.Regex
import play.api.libs.json.Json
import java.io.File

object columCrew extends App {

  val reader = CSVReader.open(new File("C:\\Users\\alexa\\Desktop\\UNIVERSIDAD\\Datasets-60922f96f3bd82568eb2bf398c404afe218f4fa0/movie_dataset.csv"))
  val data = reader.allWithHeaders()
  reader.close()

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


  val crew = data
    .map(row => row("crew"))
    .map(replacePattern2)
    .map(replacePattern)
    .map(replacePattern3)
    .map(text => text.replace("'", "\""))
    .map(text => text.replace("-u0027", "'"))
    .map(text => text.replace("-u0022", "\\\""))
    .map(text => Json.parse(text))
    .flatMap(js => js \\ "name" )
    .map(js => js.as[String])
    .distinct

  println(crew)


}

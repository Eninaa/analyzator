package ru.samis.analyzer
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import com.mongodb.client.model.Projections
import org.bson.UuidRepresentation
import org.json.JSONObject
import java.io.File



fun main(args: Array<String>) {
     Analyzer().analyzeTask(args[0])

/*
    val settings by lazy { JSONObject(File("settings.json").readText()) }
    val options by lazy { settings.getJSONObject("options") }
    val client by lazy {
        MongoClients.create(
            MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(options.getString("ConnectionString")))
                .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
                .build()
        )
    }
    val collection = client.getDatabase("rk_metadata").getCollection("userDatasets")
    val docs = collection.find().projection(Projections.include("dataset"))


*/
    //  с ошибкой в геометрии
   // Analyzer().analyzeDataset("ud_1_65365e757008d112fa24b602")

 //  Analyzer().analyzeDataset("ud_1_64b94bbde73ea056f6368e03")

//   Analyzer().analyzeDataset("ud_26_64c2afed212dea6bbe312c32")
//Analyzer().analyzeDataset("ud_1_64819ef69aa8f2443d5ea855")

  //  Analyzer().analyzeDataset("ud_1_6482c4a9089cf101b3063852")
   // Analyzer().analyzeDataset("ud_1_628e2166f844c10cc93c39f3")


   /* Analyzer().analyzeDataset("ud_1_6392eee0df203007e25ccd25")
    Analyzer().analyzeDataset("ud_1_62de4b094dbbc6789c7c6222")

    Analyzer().analyzeDataset("ud_1_64819ef69aa8f2443d5ea855")

    */
//ud_1_63a04b0bd442cb51622f6092
    // проверить
    // dataset ud_1_63d0d7c056d7b07c22533252 почему то oneVal false a db выставлено - ошибка двух признаков региона

/*
     val count = collection.countDocuments()
     var progress = 0

    var it = 0

     for (doc in docs ) {
             Analyzer().analyzeDataset(doc.getString("dataset"))
             progress++
             //        if (progress % 1000 == 0)
             println("$progress / $count")
             println()

     }
*/













}



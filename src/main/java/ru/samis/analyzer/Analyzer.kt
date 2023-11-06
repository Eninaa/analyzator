package ru.samis.analyzer

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.fge.jsonschema.core.exceptions.ProcessingException
import com.github.fge.jsonschema.core.load.configuration.LoadingConfiguration
import com.github.fge.jsonschema.main.JsonSchema
import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import com.mongodb.client.MongoCollection
import com.mongodb.client.model.*
import com.mongodb.client.model.Aggregates.*
import org.apache.commons.lang3.StringUtils
import org.bson.Document
import org.bson.UuidRepresentation
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.json.JSONArray
import org.json.JSONObject
import org.locationtech.jts.io.WKTReader
import java.io.*
import java.util.*
import kotlin.math.log2
import kotlin.math.min

class Analyzer : AutoCloseable {
    private val errors = mutableListOf<String>()
    private var progress = 0.0
    private var count = 0
    private var completed = false
    private val settings by lazy { JSONObject(File("settings.json").readText()) }
    private val params by lazy { settings.getJSONObject("params") }
    private val options by lazy { settings.getJSONObject("options") }
    private val collation: Collation = Collation.builder()
        .locale("ru")
        .collationStrength(CollationStrength.PRIMARY)
        .caseLevel(false)
        .build()

    private val dictionaries by lazy { JSONObject(File("dic.json").readText()) }

    private val client by lazy {
        MongoClients.create(
            MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(options.getString("ConnectionString")))
                .uuidRepresentation(UuidRepresentation.JAVA_LEGACY)
                .build()
        )
    }
    private lateinit var userDatasetId: String
    private var recordsToProcess: Int = -1

    private fun init(taskId: String): Document? {
        val tasksDb = client.getDatabase(options.getString("tasksDatabase"))
        val tasksDataset = tasksDb.getCollection(options.getString("tasksDataset"))

        val task = tasksDataset.find(Filters.eq("_id", ObjectId(taskId))).first() ?: run {
            writeError("task $taskId not found")
//            return null
            null
        }

        recordsToProcess = task?.getInteger("recordsToProcess") ?: params.optInt("recordsToProcess", -1)
        userDatasetId = task?.getString("dataset") ?: params.optString("dataset", null) ?: run {
            writeError("dataset id not set")
            return null
        }

        return task
    }

    fun analyzeTask(taskId: String) {
        init(taskId) ?: run {
            writeError("init error")
            return
        }

        println("analyzing task $taskId")
        analyze()
    }

    fun analyzeDataset(datasetId: String) {
        userDatasetId = datasetId
        recordsToProcess = params.optInt("recordsToProcess", -1)

        analyze()
    }

    private fun analyze() {
        println("analyzing dataset $userDatasetId")
        var time = -System.nanoTime()
        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val datasets = metadataDb.getCollection(options.getString("datasetsDataset"))

        val state = computeFieldsQuality(userDatasetId)

        (state["properties"] as Document).apply {
            put("has_address_features", hasAddressFeatures(userDatasetId))
            put("has_address", hasAddress(userDatasetId, state))
            put("connected", isConnected(state))
            put("enriched", isEnriched(userDatasetId, state))
            put("published", isPublished(userDatasetId))
        }
        datasets.updateMany(
               Filters.eq("dataset", userDatasetId),
               Updates.set("state", state)
        )
        time += System.nanoTime()
        println("analyze time ${time / 1e6}")
    }

    private fun computeFieldsQuality(dataset: String): Document {
        val wktFields = mutableMapOf<String, Int>()
        var doublesCount = 0
        val fieldsQuality = Document()
        val properties = Document()
        val result = Document().apply {
            put("fieldsQuality", fieldsQuality)
            put("properties", properties)
        }

        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val datasetsDb = client.getDatabase(options.getString("datasetsDb"))
        val structureDataset = metadataDb.getCollection(options.getString("structureDataset"))
        val datasetCollection = datasetsDb.getCollection(dataset)
        println("size ${datasetCollection.countDocuments()}")
        val struct = structureDataset.find(
            Filters.and(
                Filters.eq("database", options.getString("datasetsDb")),
                Filters.eq("dataset", dataset)
            )
        ).firstOrNull() ?: return result

        val fields = struct["fields"] as List<Document>
        val indexes = datasetCollection.listIndexes()

        //

        var oneValueRegion = false
        var regionDb = ""

        val maxCount = if (recordsToProcess > 0) recordsToProcess else datasetCollection.countDocuments().toInt()
        val countForProcessing = min(maxCount.toLong(), datasetCollection.countDocuments()).toInt()
        val basePipeline = mutableListOf<Bson>()
        if (datasetCollection.countDocuments() > maxCount)
            basePipeline += sample(maxCount)

        var regionCount = 0
        var munCount = 0

        for (field in fields) {
            var feature = ""
            val name = field.getString("name")
            if (field.containsKey("feature")) {
                feature = field.getString("feature")
            }
            if (field.isEmpty()) continue
            val type = field.getString("type").toLowerCase()
            val indexed = indexes.find { index ->
                (index["weights"] as Document?)?.containsKey(name) ?: false ||
                        (index["key"] as Document).containsKey(name)
            }

            val notEmptyPipeline =
                ArrayList(basePipeline) + match(Filters.ne(name, null)) + match(Filters.ne(name, "null"))
            val notEmptyCountPipeline = ArrayList(notEmptyPipeline) + count()
            val notEmptyCount = datasetCollection.aggregate(notEmptyCountPipeline).first()?.getInteger("count")

            val typeMatchedCount = try {
                val typeMatchingPipeline = ArrayList(notEmptyPipeline) + match(Filters.type(name, type))
                val typeMatchingCountPipeline = ArrayList(typeMatchingPipeline) + count()
                datasetCollection.aggregate(typeMatchingCountPipeline).first()?.getInteger("count")
            } catch (e: Exception) {
                null
            }

            var entropy = 0.0
            var entropyResults = listOf<Document>()

            // временный костыль от пустых полей
            if (name.isNotEmpty() && !name.equals("") && !name.equals(" ")) {
                val entropyPipeline = ArrayList(notEmptyPipeline) + group("\$$name", Accumulators.sum("count", 1)) + (sort(Document("count", -1)))
                entropyResults = datasetCollection.aggregate(entropyPipeline).allowDiskUse(true).toList()
                if (entropyResults.size > 1) {
                    if (notEmptyCount != null) {
                        for (entropyResult in entropyResults) {
                            val count = entropyResult.getInteger("count").toDouble()
                            val p = count * 1.0 / notEmptyCount.toDouble()
                            entropy -= p * log2(p)
                        }
                    }
                    entropy /= log2(entropyResults.toList().size.toDouble())
                    if (entropy > 1.0) entropy = 1.0
                }
            }

            var regionDoc = Document()
            if (feature == "Region" && regionCount == 0) {
                regionCount++
                if (entropyResults.isNotEmpty()) {
                    val regionDoc = isOneRegion(dataset, name, entropy, entropyResults)
                    oneValueRegion = regionDoc["oneValueRegion"] as Boolean
                    if (oneValueRegion) regionDb = regionDoc["database"] as String
                }
            }
            var munDoc = Document()
            if (feature == "Municipalitet" && oneValueRegion && munCount == 0) {
                munCount++
                munDoc = isOneMunicipalitet(dataset, name, entropy, entropyResults, regionDb)
            }

            fieldsQuality[name] = Document().apply {
                if (notEmptyCount != null) {
                    put("typeMatching", typeMatchedCount?.let { 1.0 * it / notEmptyCount })
                    put("entropy", entropy)
                }
                put("fullness", notEmptyCount?.let { 1.0 * it / countForProcessing })
                put("indexed", indexed != null)
                if (feature == "Region") {
                    put("oneValue", oneValueRegion)
                    if (regionDoc.isNotEmpty() && oneValueRegion) {
                        put("database", regionDoc["database"])
                    }
                } else if (feature == "Municipalitet") {
                    if(munDoc.isNotEmpty()) {
                        put("oneValue", munDoc["oneValueMunicipalitet"])
                        if (munDoc["oneValueMunicipalitet"] as Boolean) put("municipalitetName", munDoc["name"])
                    } else {
                        put("oneValue", false)
                    }
                }
            }
            when (type) {
                "geometry" -> {
                    val geometryPipeline = ArrayList(notEmptyPipeline) + project(Document(name, 1))
                    val geometries = datasetCollection.aggregate(geometryPipeline)
                    computeGeometryQuality(name, geometries, fieldsQuality)
                    properties["has_geometry"] = (fieldsQuality[name] as Document).getDouble("validness") > 0.5

                }

                "string" -> {
                    var time = -System.nanoTime()
                    val validCount = countWKT(datasetCollection, name)
                    time += System.nanoTime()
                    //println("$name checking for WKT time ${time / 1e6} count $validCount")
                    if (validCount > 0)
                        wktFields[name] = validCount
                }

                "double" -> {
                    entropy?.let {
                        if (it > 0.8)
                            doublesCount++
                    }
                }
            }
        }

        val maxWkt = wktFields.maxByOrNull { entry -> 1.0 * entry.value / countForProcessing }
        properties["has_geometry_features"] = doublesCount >= 2 || maxWkt != null

        return result
    }

    private fun countWKT(dataset: MongoCollection<Document>, field: String): Int {
        var validCount = 0
        forEachNonEmpty(dataset, field) { valueNonTyped ->
            try {
                val value = valueNonTyped as String
                if (isWKT(value))
                    validCount++
            } catch (e: Exception) {
            }
        }
        return validCount
    }

    private fun forEachNonEmpty(dataset: MongoCollection<Document>, field: String, action: (Any) -> Unit) {
        if (field.isBlank()) return
        val documents = dataset.find(Filters.ne(field, null)).projection(Projections.include(field))
        for (doc in documents) {
            doc[field]?.let(action)
        }
    }

    private val wktReader = WKTReader()
    private fun isWKT(value: String): Boolean {
        try {
            try {
                wktReader.read(value) ?: return false
                return true
            } catch (e: Exception) {
            }
        } catch (e: Exception) {
        }
        return false
    }

    private fun computeGeometryQuality(fieldName: String, geometries: Iterable<Document>, result: Document) {
        val objectMapper = ObjectMapper()
        val geoJsonSchema = getLocalGeoJsonSchema()
        val nameParts = fieldName.split(".")

        var processedCount = 0
        var valid = 0
        var time = -System.nanoTime()
        for (geometry in geometries) {
            var geoJson = geometry
            for (part in nameParts) {
                if (geoJson[part] is Document) {
                    geoJson = geoJson[part] as Document
                }
            }
            val homeGeoJson = objectMapper.readTree(geoJson.toJson())
//                    val validate = geoJsonSchema.validate(homeGeoJson)
//                    println(validate.toString())
            val validInstance = geoJsonSchema.validInstance(homeGeoJson)
//                    println(validInstance)
            if (validInstance) valid++
            processedCount++
//            if (processedCount % 10000 == 0) println(processedCount)
        }
        time += System.nanoTime()
        println("geometry validation time ${time / 1e6}")
        (result[fieldName] as Document)["validness"] = if (processedCount > 0) 1.0 * valid / processedCount else 0.0
    }


    private fun isEnriched(dataset: String, state: Document): Boolean {
        var result = false

        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val structureDataset = metadataDb.getCollection(options.getString("structureDataset"))

        val struct = structureDataset.find(
            Filters.and(
                Filters.eq("database", options.getString("datasetsDb")),
                Filters.eq("dataset", dataset)
            )
        ).firstOrNull() ?: return false

        val fields = struct["fields"] as List<Document>

        for (field in fields) {
            if (field.getString("name").equals("oarObject")) {
                val fieldsQuality: Document? = state["fieldsQuality"] as? Document
                val oarFullness = (fieldsQuality?.get("oarObject") as? Document)?.getDouble("fullness")
                if (oarFullness != null && oarFullness > 0.7) {
                    result = true
                    break
                }
            }
        }
        return result
    }

    private fun isPublished(dataset: String): Boolean {
        var result = false

        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val userDatasets = metadataDb.getCollection(options.getString("datasetsDataset"))
        val dataset = userDatasets.find(
            Filters.eq("dataset", dataset)
        ).firstOrNull() ?: return false
        if (dataset.containsKey("geoportalLayerId")) return true
        return result
    }

    private fun isConnected(state: Document): Boolean {
        var connected = false
        val fieldsQuality: Document? = state["fieldsQuality"] as? Document
        val properties = state["properties"] as Document
        val hasAddress = properties["has_address"] as? Boolean
        val hasGeometry = properties["has_geometry"] as? Boolean
        val oarFullness = (fieldsQuality?.get("oarObject") as? Document)?.getDouble("fullness")

        if (oarFullness != null) {
            if (oarFullness < 0.6) return false
            else if (hasAddress == true || hasGeometry == true) connected = true
        }
        return connected

    }

    private fun hasAddressFeatures(dataset: String): Boolean {
        var result = false
        val dictionary by lazy { dictionaries.getJSONArray("dic") }

        val datasetsDb = client.getDatabase(options.getString("datasetsDb"))
        val datasetCollection = datasetsDb.getCollection(dataset)

        val maxCount = if (recordsToProcess > 0) recordsToProcess else datasetCollection.countDocuments().toInt()
        val count = min(maxCount.toLong(), datasetCollection.countDocuments()).toInt()

        val basePipeline =
            if (datasetCollection.countDocuments() > maxCount) listOf(sample(maxCount)) else listOf<Bson>()

        val fieldsWithTypeString = ArrayList(basePipeline)
            .plus(project(Document("fieldsWithTypeString", Document("\$filter",
                            Document("input", Document("\$objectToArray", "\$\$ROOT"))
                                .append("as", "field")
                                .append("cond", Document("\$eq", listOf(Document("\$type", "\$\$field.v"), "string")))))))

        val fieldsWithEnoughWords = ArrayList(fieldsWithTypeString)
            .plus(addFields(Field("fieldsWithEnoughWords", Document("\$filter", Document("input", "\$fieldsWithTypeString")
                                .append("as", "field")
                                .append("cond", Document("\$gt", listOf(Document("\$size", Document("\$split", listOf("\$\$field.v", " "))), 3)))))))
            .plus(unwind("\$fieldsWithEnoughWords"))

        val groupedByKeys = ArrayList(fieldsWithEnoughWords).plus(
            group("\$fieldsWithEnoughWords.k", Accumulators.sum("count", 1))
        ).plus(sort(Document("count", -1)))

        val selectedKeys = datasetCollection.aggregate(groupedByKeys)
            .filter { it["count"] as Int >= count / 2 }
            .map { it["_id"] as String }
            .toList()

        if (selectedKeys.isEmpty()) return false
        val processedFields = ArrayList(basePipeline)
            .plus(project(Projections.excludeId()))
            .plus(project((Projections.include(selectedKeys))))
            .plus(project(Document("fields", Document("\$objectToArray", "\$\$ROOT"))))
            .plus(addFields(Field("processedFields", Document("\$map", Document("input", "\$fields")
                                .append("as", "field")
                                .append("in", Document("k", "\$\$field.k")
                                        .append("v", Document("\$replaceAll", Document("input", "\$\$field.v")
                                                    .append("find", ",")
                                                    .append("replacement", " "))))))))
            .plus(addFields(Field("processedFields", Document("\$map", Document("input", "\$processedFields")
                                .append("as", "field")
                                .append("in", Document("k", "\$\$field.k")
                                        .append("v", Document("\$replaceAll", Document("input", "\$\$field.v")
                                                    .append("find", ".")
                                                    .append("replacement", " "))))))))
            .plus(addFields(Field("processedFields", Document("\$map", Document("input", "\$processedFields")
                                .append("as", "field")
                                .append("in", Document("k", "\$\$field.k")
                                    .append("v", Document("\$replaceAll", Document("input", "\$\$field.v")
                                                    .append("find", "  ")
                                                    .append("replacement", " "))))))))
            .plus(unwind("\$processedFields"))
            .plus(match(Filters.ne("processedFields.v", null)))
            .plus(addFields(Field("result", Document("\$gte", listOf(Document("\$size", Document("\$setIntersection",
                                        listOf(Document("\$split", listOf("\$processedFields.v", " ")), dictionary))), 2)))))
            .plus(group("\$processedFields.k", Accumulators.sum("count", Document("\$cond", Document("if", "\$result")
                                .append("then", 1)
                                .append("else", 0)))))
            .plus(sort(Document("count", -1)))

        val resultingPipeline = datasetCollection.aggregate(processedFields).collation(collation).toList()

        resultingPipeline.forEach {
            if (it["count"] as Int >= count / 2) {
                result = true
            }
        }
        return result
    }

    private fun hasAddress(dataset: String, state: Document): Boolean {
        val datasetsDb = client.getDatabase(options.getString("datasetsDb"))
        val datasetCollection = datasetsDb.getCollection(dataset)

        val maxCount = if (recordsToProcess > 0) recordsToProcess else datasetCollection.countDocuments().toInt()
        val count = min(maxCount.toLong(), datasetCollection.countDocuments()).toInt()

        val basePipeline =
            if (datasetCollection.countDocuments() > maxCount) listOf(sample(maxCount)) else listOf<Bson>()

        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val structureDataset = metadataDb.getCollection(options.getString("structureDataset"))

        val struct = structureDataset.find(
            Filters.and(
                Filters.eq("database", options.getString("datasetsDb")),
                Filters.eq("dataset", dataset)
            )
        ).firstOrNull() ?: return false

        val fields = struct["fields"] as List<Document>
        val addressParts = listOf("Region", "Municipalitet", "Street", "HouseNumber")
        val fieldNames = fields.filter { it.getString("feature") in addressParts }
            .associate { it.getString("feature") to it.getString("name") }
        // key addressParts value fieldName in db
        var nonEmptyMun = 0.0
        var nonEmptyStreetAndHouse = 0.0

        if (fieldNames.size == 4) {

            val notEmptyMunPipeline =
                ArrayList(basePipeline).plus(match(Filters.ne(fieldNames["Municipalitet"], null)))
            val notEmptyMunCountPipeline = ArrayList(notEmptyMunPipeline).plus(count())
            val notEmptyMunCount = datasetCollection.aggregate(notEmptyMunCountPipeline).first()?.getInteger("count")
            if (notEmptyMunCount != null) {
                nonEmptyMun = notEmptyMunCount.toDouble() / count
            }
            val notEmptyStreetAndHousePipeline = ArrayList(basePipeline)
                .plus(match(Filters.and(Filters.ne(fieldNames["Street"], null), Filters.ne(fieldNames["HouseNumber"], null))))
            val notEmptyStreetAndHouseCountPipeline = ArrayList(notEmptyStreetAndHousePipeline).plus(count())
            val notEmptyStreetAndHouseCount =
                datasetCollection.aggregate(notEmptyStreetAndHouseCountPipeline).first()?.getInteger("count")
            if (notEmptyStreetAndHouseCount != null) {
                nonEmptyStreetAndHouse = notEmptyStreetAndHouseCount.toDouble() / count
            }
        }
        if (nonEmptyMun > 0.6 && nonEmptyStreetAndHouse > 0.6) {
            return true
        }
        return false
    }

    private fun isOneRegion(dataset: String, fieldName: String, regionEntropy: Double, entropyPipeline: List<Document>): Document{
        var oneValueRegion = false
        var regionDatabase: String
        var regionFieldValue = ""
        var state = Document()
        val array by lazy { dictionaries.getJSONArray("regionTypes") }

        if (regionEntropy < 0.05) {
            oneValueRegion = true
            regionFieldValue = entropyPipeline[0]["_id"] as String
            regionFieldValue = processString(regionFieldValue, array)
        } else if (regionEntropy < 0.5) {
            val result = findSameFieldValues(dataset, fieldName, "regionTypes")
            if (result[0] as Boolean) {
                oneValueRegion = result[0] as Boolean
                regionFieldValue= result[1] as String
            }
        }
        state["oneValueRegion"] = oneValueRegion
        if (oneValueRegion) {
            regionDatabase = findRegionDatabase(regionFieldValue)
            state["database"] = regionDatabase
        }
        return state
    }

    private fun isOneMunicipalitet(dataset: String, fieldName: String, municipalitetEntropy: Double, entropyPipeline: List<Document>, regionDB: String): Document{
        var oneValueMunicipalitet = false
        var municipalitetName: String
        var municipalitetFieldValue = ""
        var state = Document()
        val array by lazy { dictionaries.getJSONArray("municipalitetTypes") }

        if (municipalitetEntropy < 0.05) {
            oneValueMunicipalitet = true
            municipalitetFieldValue = entropyPipeline[0]["_id"] as String
            municipalitetFieldValue = processString(municipalitetFieldValue, array)

        } else if (municipalitetEntropy < 0.4) {
            val result = findSameFieldValues(dataset, fieldName, "municipalitetTypes")
            if (result[0] as Boolean) {
                oneValueMunicipalitet = result[0] as Boolean
                municipalitetFieldValue = result[1] as String
            }
        }
        state["oneValueMunicipalitet"] = oneValueMunicipalitet
        if (oneValueMunicipalitet) {
            municipalitetName = findMunicipalitet(regionDB, municipalitetFieldValue)
            state["municipalitetName"] = municipalitetName
        }
        return state
    }

    private fun findSameFieldValues(dataset: String, fieldName: String, addressLevel: String): List<Any> {

        val array by lazy { dictionaries.getJSONArray(addressLevel) }
        var oneValue = false
        var field = ""
        val datasetsDb = client.getDatabase(options.getString("datasetsDb"))
        val datasetCollection = datasetsDb.getCollection(dataset)

        val maxCount = if (recordsToProcess > 0) recordsToProcess else datasetCollection.countDocuments().toInt()
        val count = min(maxCount.toLong(), datasetCollection.countDocuments()).toInt()
        val basePipeline =
            if (datasetCollection.countDocuments() > maxCount) listOf(sample(maxCount)) else listOf<Bson>()

        val pipeline = ArrayList(basePipeline)
            .plus(group("\$$fieldName", Accumulators.sum("count", 1)))
            .plus(project(Document("_id", 1)
                        .append(fieldName, "\$_id")
                        .append("count", "\$count")))
            .plus(project(Document("_id", 1)
                        .append(fieldName, "\$_id")
                        .append("count", "\$count")
                        .append("normalizedName", Document("\$replaceAll", Document("input", Document("\$replaceAll", Document("input", "\$$fieldName")
                                            .append("find", ".")
                                            .append("replacement", " ")))
                                    .append("find", ",")
                                    .append("replacement", " ")))))
            .plus(project(Document("_id", 1)
                        .append(fieldName, "\$_id")
                        .append("count", "\$count")
                        .append("normalizedName", Document("\$replaceAll", Document("input", "\$normalizedName")
                                    .append("find", "  ")
                                    .append("replacement", " ")))))
            .plus(addFields(Field("splittedNormalizedName", Document("\$split", listOf("\$normalizedName", " ")))))
            .plus(addFields(Field("res", Document("\$filter", Document("input", "\$splittedNormalizedName")
                                .append("as", "arr")
                                .append("cond", Document("\$not", Document("\$in", listOf("\$\$arr", array))))))))
            .plus(addFields(Field("concatenatedResult", Document("\$reduce", Document("input", "\$res")
                                .append("initialValue", "")
                                .append("in", Document("\$cond", Document("if", Document("\$eq", listOf("\$\$value", "")))
                                            .append("then", "\$\$this")
                                            .append("else", Document("\$concat", listOf("\$\$value", " ", "\$\$this")))))))))

        val res = datasetCollection.aggregate(pipeline).collation(collation).toList()
        var resultingDoc = mutableListOf<Document>()
        res.forEach{
            var doc = Document()
            doc[fieldName] = it[fieldName]
            doc["field"] = it["concatenatedResult"]
            doc["count"] = it["count"]
            resultingDoc.add(doc)
        }
        var result = mutableListOf<Document>()
        var k = -1
        for (i in 0 until resultingDoc.size) {
            if (i != k) {
                val field1 = resultingDoc[i]["field"] as? String
                for (j in 0 until resultingDoc.size) {
                    if (i != j) {
                        val field2 = resultingDoc[j]["field"] as? String
                        if (field1 != null && field2 != null) {
                            val sim = findSimilarity(field1, field2)
                            if (sim > 0.8) {
                                k = j
                                var doc = Document()
                                doc[fieldName] = resultingDoc[i][fieldName] as String
                                doc["count"] = resultingDoc[i]["count"] as Int + resultingDoc[j]["count"] as Int
                                result.add(doc)
                            }
                        }
                    }
                }
            }
        }
        if (result.size == 1) {
            if (result[0]["count"] as Int > count * 0.9) {
                oneValue = true
                field = result[0][fieldName] as String
            }
        }
        return listOf(oneValue, field)
    }

    private fun findRegionDatabase (regionFieldValue: String): String {
        var regionDatabase = ""
        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val regionState = metadataDb.getCollection("regionState")
        val array by lazy { dictionaries.getJSONArray("regionTypes") }

        val pipeline = listOf(
            project(Projections.include("region", "database")),
            addFields(Field("regionArr", Document("\$split", listOf("\$region", " ")))),
            addFields(Field("res", Document("\$filter", Document("input", "\$regionArr")
                .append("as", "arr")
                .append("cond", Document("\$not", Document("\$in", listOf("\$\$arr", array))))))),
            match(Document("res", Document("\$not", Document("\$elemMatch", Document("\$eq", "(debug)"))))),
            addFields(Field("concatenatedResult", Document("\$reduce", Document("input", "\$res")
                .append("initialValue", "")
                .append("in", Document("\$cond", Document("if", Document("\$eq", listOf("\$\$value", "")))
                    .append("then", "\$\$this")
                    .append("else", Document("\$concat", listOf("\$\$value", " ", "\$\$this")))))))))

        val res = regionState.aggregate(pipeline).collation(collation).toList()
        var max = 0.0
        res.forEach {
            var sim = findSimilarity(regionFieldValue, it["concatenatedResult"] as String)
            if (sim > max && sim > 0.9) {
                max = sim
                regionDatabase = it["database"] as String
            }
        }
        return regionDatabase
    }

    private fun findMunicipalitet(regionFieldDB: String, municipalitetFieldValue: String): String{

        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val regionState = metadataDb.getCollection("regionState")
        val regionFieldValue = regionState.aggregate(listOf(match(Filters.eq("database", regionFieldDB)))).first().getString("region")

        val array by lazy { dictionaries.getJSONArray("municipalitetTypes") }

        val rkCommon = client.getDatabase("rk_common")
        val municipalitets = rkCommon.getCollection("municipalitets")
        var municipalitetName = ""

        val pipeline = listOf(
            match(Filters.eq("region", regionFieldValue)),
            project(Projections.include("region", "name")),
            addFields(Field("munArr", Document("\$split", listOf("\$name", " ")))),
            addFields(Field("res", Document("\$filter", Document("input", "\$munArr")
                .append("as", "arr")
                .append("cond", Document("\$not", Document("\$in", listOf("\$\$arr", array))))))),
            addFields(Field("concatenatedResult", Document("\$reduce", Document("input", "\$res")
                .append("initialValue", "")
                .append("in", Document("\$cond", Document("if", Document("\$eq", listOf("\$\$value", "")))
                    .append("then", "\$\$this")
                    .append("else", Document("\$concat", listOf("\$\$value", " ", "\$\$this")))))))))

        val result = municipalitets.aggregate(pipeline).collation(collation).toList()
        var max = 0.0
        result.forEach {
            var sim = findSimilarity(municipalitetFieldValue, it["concatenatedResult"] as String)
            if (sim > max && sim > 0.9) {
                max = sim
                municipalitetName = it["name"] as String
            }
        }
        return municipalitetName
    }

    private fun processString(input: String, wordsToRemove: JSONArray): String {
        val cleanedString = input.replace("[.,]".toRegex(), " ").replace("\\s+".toRegex(), " ")
        val words = cleanedString.split(" ")
        val filteredWords = words.filter { it !in wordsToRemove }
        return filteredWords.joinToString(" ")
    }

    private fun findSimilarity(x: String, y: String): Double {
        val maxLength = java.lang.Double.max(x.length.toDouble(), y.length.toDouble())
        return if (maxLength > 0) {
            (maxLength - StringUtils.getLevenshteinDistance(
                x.lowercase(Locale.getDefault()),
                y.lowercase(Locale.getDefault())
            )) / maxLength
        } else 1.0
    }

    override fun close() {
        client.close()
    }

    protected fun writeComplete(count: Int) {
        completed = true
        writeProgress(1.0, count)
    }

    protected fun writeProgress(progress: Double, count: Int) {
        this.progress = progress
        this.count = count
        flush()
    }

    protected fun incCount(countInc: Int = 1) {
        this.count += countInc
        flush()
    }

    protected fun writeError(descr: String) {
        errors += "\n" + descr
        flush()
    }

    private fun flush() {
        val err = JSONArray()
        for (error in errors) {
            err.put(error)
        }
        FileWriter("info.json").use {
            it.write(
                JSONObject()
                    .put("progress", progress)
                    .put("inserted", count)
                    .put("errors", err)
                    .put("completed", completed)
                    .toString()
            )
        }
    }

    companion object {
        @Throws(IOException::class, ProcessingException::class)
        fun getLocalGeoJsonSchema(): JsonSchema {
            val objectMapper = ObjectMapper()
            val baseSchemaPath = "schemas/sample-json-schemas-master/geojson/"
            val geoJsonSchema: InputStream =
                FileInputStream(File(String.format("%sgeojson.json", baseSchemaPath)))
            val geometrySchema: InputStream =
                FileInputStream(File(String.format("%sgeometry.json", baseSchemaPath)))
            val crsSchema: InputStream =
                FileInputStream(File(String.format("%scrs.json", baseSchemaPath)))
            val bboxSchema: InputStream =
                FileInputStream(File(String.format("%sbbox.json", baseSchemaPath)))
            val geoJsonSchemaNode = objectMapper.readTree(geoJsonSchema)
            val geometrySchemaNode = objectMapper.readTree(geometrySchema)
            val crsSchemaNode = objectMapper.readTree(crsSchema)
            val bboxSchemaNode = objectMapper.readTree(bboxSchema)
            val loadingCfg = LoadingConfiguration.newBuilder()
                .preloadSchema(geoJsonSchemaNode)
                .preloadSchema(geometrySchemaNode)
                .preloadSchema(crsSchemaNode)
                .preloadSchema(bboxSchemaNode)
                .setEnableCache(true)
                .freeze()
            val jsonSchemaFactory = JsonSchemaFactory.newBuilder()
                .setLoadingConfiguration(loadingCfg)
                .freeze()
            return jsonSchemaFactory.getJsonSchema(geoJsonSchemaNode)
        }
    }
}
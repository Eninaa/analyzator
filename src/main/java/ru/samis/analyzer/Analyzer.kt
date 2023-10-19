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
import org.bson.Document
import org.bson.UuidRepresentation
import org.bson.conversions.Bson
import org.bson.types.ObjectId
import org.json.JSONArray
import org.json.JSONObject
import org.locationtech.jts.io.WKTReader
import java.io.*
import java.util.ArrayList
import kotlin.Exception
import kotlin.math.ln
import kotlin.math.min

class Analyzer : AutoCloseable {
    private val errors = mutableListOf<String>()
    private var progress = 0.0
    private var count = 0
    private var completed = false
    private val settings by lazy { JSONObject(File("settings.json").readText()) }
    private val params by lazy { settings.getJSONObject("params") }
    private val options by lazy { settings.getJSONObject("options") }
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
            put("has_address_features", true)
            put("has_address", true)
            put("connected", true)
            put("enriched", isEnriched(userDatasetId))
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

        val maxCount = if (recordsToProcess > 0) recordsToProcess else datasetCollection.countDocuments().toInt()
        val countForProcessing = min(maxCount.toLong(), datasetCollection.countDocuments()).toInt()
        val basePipeline = mutableListOf<Bson>()
        if (datasetCollection.countDocuments() > maxCount)
            basePipeline += Aggregates.sample(maxCount)

        for (field in fields) {
            val name = field.getString("name")
            if (field.isEmpty()) continue
            val type = field.getString("type").toLowerCase()
            val indexed = indexes.find { index ->
                (index["weights"] as Document?)?.containsKey(name) ?: false ||
                        (index["key"] as Document).containsKey(name)
            }

            val notEmptyPipeline = ArrayList(basePipeline) + Aggregates.match(Filters.ne(name, null))
            val notEmptyCountPipeline = ArrayList(notEmptyPipeline) + Aggregates.count()
            val notEmptyCount = datasetCollection.aggregate(notEmptyCountPipeline).first()?.getInteger("count")

            val typeMatchedCount = try {
                val typeMatchingPipeline = ArrayList(notEmptyPipeline) + Aggregates.match(Filters.type(name, type))
                val typeMatchingCountPipeline = ArrayList(typeMatchingPipeline) + Aggregates.count()
                datasetCollection.aggregate(typeMatchingCountPipeline).first()?.getInteger("count")
            } catch (e: Exception) {
                null
            }

            val entropyPipeline = ArrayList(notEmptyPipeline) + Aggregates.group(name, Accumulators.sum("count", 1))
            val entropyCount = datasetCollection.aggregate(entropyPipeline).first()?.getInteger("count")
            val entropy = notEmptyCount?.let {
                entropyCount?.let {
                    val entropy = 1.0 * it / notEmptyCount
                    entropy - entropy * ln(entropy)
                }
            }

            fieldsQuality[name] = Document().apply {
                if (notEmptyCount != null) {
                    put("typeMatching", typeMatchedCount?.let { 1.0 * it / notEmptyCount })
                    put("entropy", entropy)
                }
                put("fullness", notEmptyCount?.let { 1.0 * it / countForProcessing })
                put("indexed", indexed != null)
            }

            when (type) {
                "geometry" -> {
                    val geometryPipeline = ArrayList(notEmptyPipeline) + Aggregates.project(Document(name, 1))
                    val geometries = datasetCollection.aggregate(geometryPipeline)
                    computeGeometryQuality(name, geometries, fieldsQuality)
                    properties["has_geometry"] = (fieldsQuality[name] as Document).getDouble("validness") > 0.5
                }

                "string" -> {
                    var time = -System.nanoTime()
                    val validCount = countWKT(datasetCollection, name)
                    time += System.nanoTime()
                    println("$name checking for WKT time ${time / 1e6} count $validCount")
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

    private fun isXY(value: Double) {

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
                geoJson = geoJson[part] as Document
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

        (result[fieldName] as Document)["validness"] = if (processedCount > 0) 1.0 * valid / processedCount else 0
    }


    private fun isEnriched(dataset: String): Boolean {
        var result = false

        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val structureDataset = metadataDb.getCollection(options.getString("structureDataset"))

        val struct = structureDataset.find(
            Filters.and(
                Filters.eq("database", options.getString("datasetsDb")),
                Filters.eq("dataset", dataset)
            )
        ).firstOrNull() ?: return result

        val fields = struct["fields"] as List<Document>

        for (field in fields) {
            if (field.getString("name").equals("oarObject")) {
                result = true
                break
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
        ).firstOrNull() ?: return result
        if (dataset.containsKey("geoportalLayerId")) result = true

        return result
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
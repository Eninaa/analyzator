package ru.samis.analyzer

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.fge.jsonschema.core.exceptions.ProcessingException
import com.github.fge.jsonschema.core.load.configuration.LoadingConfiguration
import com.github.fge.jsonschema.main.JsonSchema
import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.mongodb.ConnectionString
import com.mongodb.MongoClientSettings
import com.mongodb.client.MongoClients
import com.mongodb.client.model.*
import org.bson.Document
import org.bson.UuidRepresentation
import org.bson.conversions.Bson
import org.json.JSONArray
import org.json.JSONObject
import java.io.*
import java.lang.Exception
import java.util.ArrayList
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

    fun analyze(dataset: String) {
        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val datasets = metadataDb.getCollection(options.getString("datasetsDataset"))

        val state = Document().apply {
            put("properties", Document().apply {
                put("has_address_features", true)
                put("has_geometry", true)
                put("has_address", true)
                put("connected", true)
                put("enriched", true)
                put("published", true)
            })
            put("fieldsQuality", computeFieldsQuality(dataset))
        }

        datasets.updateMany(
            Filters.eq("dataset", dataset),
            Updates.set("state1", state)
        )
    }

    private fun computeFieldsQuality(dataset: String): Document {
        val result = Document()
        val metadataDb = client.getDatabase(options.getString("metadataDb"))
        val datasetsDb = client.getDatabase(options.getString("datasetsDb"))
        val structureDataset = metadataDb.getCollection(options.getString("structureDataset"))
        val datasetCollection = datasetsDb.getCollection(dataset)
        val struct = structureDataset.find(
            Filters.and(
                Filters.eq("database", options.getString("datasetsDb")),
                Filters.eq("dataset", dataset)
            )
        ).firstOrNull() ?: return result

        val fields = struct["fields"] as List<Document>
        val indexes = datasetCollection.listIndexes()

        val maxCount = 10000
        val count = min(maxCount.toLong(), datasetCollection.countDocuments()).toInt()
        val basePipeline =
            if (datasetCollection.countDocuments() > maxCount) listOf(Aggregates.sample(maxCount)) else listOf<Bson>()

        for (field in fields) {
            val name = field.getString("name")
            val type = field.getString("type").toLowerCase()
            val indexed = indexes.find { index ->
                (index["weights"] as Document?)?.containsKey(name) ?: false ||
                        (index["key"] as Document).containsKey(name)
            }

            val notEmptyPipeline = ArrayList(basePipeline).plus(Aggregates.match(Filters.ne(name, null)))
            val notEmptyCountPipeline = ArrayList(notEmptyPipeline).plus(Aggregates.count())
            val notEmptyCount = datasetCollection.aggregate(notEmptyCountPipeline).first()?.getInteger("count")

            val typeMatchedCount = try {
                val typeMatchingPipeline =
                    ArrayList(notEmptyPipeline).plus(Aggregates.match(Filters.type(name, type)))
                val typeMatchingCountPipeline = ArrayList(typeMatchingPipeline).plus(Aggregates.count())
                datasetCollection.aggregate(typeMatchingCountPipeline).first()?.getInteger("count")
            } catch (e: Exception) {
                null
            }

            val entropyPipeline =
                ArrayList(notEmptyPipeline).plus(Aggregates.group(name, Accumulators.sum("count", 1)))
            val entropyCount = datasetCollection.aggregate(entropyPipeline).first()?.getInteger("count")

            result[name] = Document().apply {
                if (notEmptyCount != null) {
                    put("typeMatching", typeMatchedCount?.let { 1.0 * it / notEmptyCount })
                    put("entropy", entropyCount?.let {
                        val entropy = 1.0 * it / notEmptyCount
                        entropy - entropy * ln(entropy)
                    })
                }
                put("fullness", notEmptyCount?.let { 1.0 * it / count })
                put("indexed", indexed != null)
            }

            if (type == "geometry") {
                val geometryPipeline =
                    ArrayList(notEmptyPipeline).plus(Aggregates.project(Document(name, 1)))
                val geometries = datasetCollection.aggregate(geometryPipeline)
                computeGeometryQuality(name, geometries, result)
            }
        }

        return result
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
            if (processedCount % 10000 == 0) println(processedCount)
        }
        time += System.nanoTime()
        println("geometry validation time ${time / 1e6}")

        (result[fieldName] as Document)["validness"] = if (processedCount > 0) 1.0 * valid / processedCount else 0
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
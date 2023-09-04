package ru.samis.analyzer

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.fge.jsonschema.core.exceptions.ProcessingException
import com.github.fge.jsonschema.core.load.configuration.LoadingConfiguration
import com.github.fge.jsonschema.main.JsonSchema
import com.github.fge.jsonschema.main.JsonSchemaFactory
import com.mongodb.BasicDBObject
import com.mongodb.client.MongoClients
import org.json.JSONObject
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.io.InputStream


// есть ли вообще поля, похожие на адрес
// идея пайплайна - выбрать строковые поля, из них выбрать поля в которых >= 3 слов (разделитель - пробел), из оставшихся выбрать те в которых встречаются слова из словарей
// в классе Pipelines лежит часть пайплайна для этого метода
fun hasAddressFeatures(): Boolean {
    return false
}

// есть ли вообще поля, похожие на необработанную геометрию (WKT или x/y)
//
fun hasGeometryFeatures(): Boolean {
    return false
}

// есть ли набор полей адресов с каким-то минимальным уровнем полноты
//
fun hasAddress(): Boolean {
    return false
}

// есть ли нормальное поле геометрии с каким-то минимальным уровнем полноты и других качеств
fun hasGeometry(): Boolean {
    return false
}

val isConnected: Boolean
    // связан на какую-то минимальную долю
    get() = false


fun fieldsEstimation() {
    /*
    в rk_metadata.datasetsStructure лежит описание полей
    typeMatching - какая доля соответствует декларированному типу


    fullness - какая доля непустых

    оценивать по уникальным значением (можно использовать groupBy)
    entropy - мера разнообразия (0 - все одинаковые, 1 - все разные) среди непустых


    indexed - существует индекс на это поле

    можно использовать geotools
    validness - только для геометрии, доля валидных по правилам GeoJSON среди непустых



    adequacy - только для геометрии, доля адекватных значений координат (территории России и вообще) среди непустых



    // для мalidness
    public Geometry GeoJsonToWkt(String geoJson) {
    GeometryJSON geometryJSON = new GeometryJSON();
    try {
        return geometryJSON.read(geoJson);
    } catch (Exception e) {
        return null;
    }
}

    */
}


fun main(args: Array<String>) {
    println(args[0])
    Analyzer().analyze(args[0])


    /*
    разбор адреса: !has_address && has_address_features
    трансформация геометрии: !has_geometry && has_geometry_features
    связывание: !connected && (has_geometry || has_address)
    публикация: has_geometry
    показать на карте: published
    настройка метаданных: true
    экспорт: true
    */
}



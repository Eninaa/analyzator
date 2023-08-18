import com.mongodb.BasicDBObject;
import com.mongodb.client.*;
import org.bson.Document;
import org.json.JSONObject;

import java.util.ArrayList;


public class Main {
    // есть ли вообще поля, похожие на адрес
    // идея пайплайна - выбрать строковые поля, из них выбрать поля в которых >= 3 слов (разделитель - пробел), из оставшихся выбрать те в которых встречаются слова из словарей
    public static boolean hasAddressFeatures() {
        boolean hasAddressFeatures = false;
        return hasAddressFeatures;
    }

    // есть ли вообще поля, похожие на необработанную геометрию (WKT или x/y)
    //
    public static boolean hasGeometryFeatures() {
        boolean hasGeometryFeatures = false;
        return hasGeometryFeatures;
    }

    // есть ли набор полей адресов с каким-то минимальным уровнем полноты
    //
    public static boolean hasAddress() {
        boolean hasAddress = false;
        return hasAddress;
    }

    // есть ли нормальное поле геометрии с каким-то минимальным уровнем полноты и других качеств
    public static boolean hasGeometry() {
        boolean hasGeometry = false;
        return hasGeometry;
    }

    // связан на какую-то минимальную долю
    public static boolean isConnected() {
        boolean isConnected = false;
        return isConnected;
    }

    // есть ли обогащающие поля из ОАП
    public static boolean isEnriched() {
        boolean isEnriched = false;
        return isEnriched;
    }

    // был опубликован на карту
    public static boolean isPublished() {
        boolean isPublished = false;
        return isPublished;
    }

    public static void fieldsEstimation() {
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


    public static void main(String[] args) {

        // вынести в конфиг ?
        String[] dict = {
                "область", "обл", "край", "регион",
                "район", "р-н",
                "поселок", "посёлок", "пос", "поселок городского типа", "посёлок городского типа", "пгт",
                "село", "деревня", "город", "гор",
                "улица", "ул", "переулок", "бульвар", "бул", "проспект", "пр",
                "дом", "корпус", "строение", "здание"};

        String[] regionTypes = {
                "область", "обл", "край", "регион"
        };

        String[] municipalitetTypes = {
                "район", "р-н",
                "поселок", "посёлок", "пос",
                "поселок городского типа", "посёлок городского типа", "пгт",
                "село", "деревня",
                "город", "гор"};

        String[] streetTypes = {
                "аллея", "ал",
                "бульвар", "бул",
                "дорога", "дор",
                "кольцевая",
                "набережная",
                "переулок", "пер",
                "площадь", "пл",
                "проезд",
                "проспект", "пр",
                "линия", "лин",
                "шоссе", "ш", "шоссейная",
                "улица", "ул"
        };

        String[] houseTypes = {
                "дом", "д",
                "здание",
                "строение", "стр",
                "корпус", "корп", "к"
        };

        String[] geometryTypesWKT = {
                "Point", "LineString", "Polygon", "GeometryCollection",
                "MultiPoint", "MultiLineString", "MultiPolygon",
                "Curve", "Circle", "Surface",
                "CircularString", "CompoundCurve", "CurvePolygon", "MultiCurve",
                "MultiSurface", "PolyhedralSurface", "TIN", "Triangle", "GeodesicString",
                "EllipticalCurve", "NurbsCurve", "Clothoid", "SpiralCurve", "CompoundSurface", "BrepSolid", "AffinePlacement"
        };


        //dataset: "ud_1_640b08cb9c09ed20680f7682"}


        MongoClient mongo = MongoClients.create("mongodb://192.168.57.102:27017");

        // в конфиге указывать бд + коллекцию
        MongoDatabase rk_userDatasets = mongo.getDatabase("rk_userDatasets");
        MongoDatabase rk_metadata = mongo.getDatabase("rk_metadata");

        MongoCollection<Document> collection = rk_userDatasets.getCollection("ud_1_640b08cb9c09ed20680f7682");

        BasicDBObject filter = new BasicDBObject();
        filter.put("dataset", "ud_1_640b08cb9c09ed20680f7682");
        MongoCursor<Document> datasetStructure = rk_metadata.getCollection("datasetsStructure").find(filter).cursor();
        ArrayList<JSONObject> fields;

        while (datasetStructure.hasNext()) {
            Document doc = datasetStructure.next();
            fields = (ArrayList<JSONObject>) doc.get("fields");
        }






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
}

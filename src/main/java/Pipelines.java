public class Pipelines {
    // hasAddressFeatures
    /*
    Arrays.asList(new Document("$project",
    new Document("fieldsWithTypeString",
    new Document("$filter",
    new Document("input",
    new Document("$objectToArray", "$$ROOT"))
                    .append("as", "field")
                    .append("cond",
    new Document("$eq", Arrays.asList(new Document("$type", "$$field.v"), "string")))))),
    new Document("$addFields",
    new Document("fieldsWithEnoughWords",
    new Document("$filter",
    new Document("input", "$fieldsWithTypeString")
                    .append("as", "field")
                    .append("cond",
    new Document("$gte", Arrays.asList(new Document("$size",
                            new Document("$split", Arrays.asList("$$field.v", " "))), 3L)))))))
     */
}

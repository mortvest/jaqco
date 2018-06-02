import CodeGenFunctions._

object DDLProcessor {
  def apply(
    meta: Map[String, TableMetaData],
    schemaFileName: String,
    typeDefFileName: String,
    namespace: String
  ) = {
    val schemaCreation = genSchema(meta, "jaqco", typeDefFileName, namespace)
    val typeCreation = genTypeDef(meta, namespace)
    (schemaCreation, typeCreation)
  }

  def genSchema(
    metaMap: Map[String, TableMetaData],
    schemaName: String,
    typeDefFileName: String,
    namespace: String
  ) = {
      def genIndex(indexName: String, typeName: String): String = {
        s"""my_database->create_index("$indexName", sizeof($typeName), false);"""
      }
      s"""#include "$typeDefFileName"
namespace $namespace {
  class ${schemaName}_schema_creator: public reactdb::abstract_schema_creator {
  public:
      void create_schema(reactdb::hypo_db* my_database) override {
${metaMap.foldLeft("")( (acc, x) => acc + "        " + genIndex(x._1, x._1 + "_key_type") + "\n" )}
      }
  }
  ;
}"""
  }
  def genTypeDef(metaMap: Map[String, TableMetaData], namespace: String): String = {
    def newKeyStruct(meta: TableMetaData, valType: String) = {
      val newMap = meta.attributes.filter { case x => meta.indexParts.contains(x._1) }
      val header = s"struct ${valType} {\n"
      val fields = newMap.foldLeft("") ((acc, x) => acc + "  " + structElem(x) + "\n")
      val endianConvertion = ""
      header + fields + endianConvertion + "\n};"
    }
    def proc(meta: TableMetaData) = {
      val attributes = meta.attributes
      val indexParts = meta.indexParts
      val name = meta.relName
      val keyStruct = newKeyStruct(meta, name+"_key_type")
      val valStruct = newValueStruct(meta, name+"_val_type")
      keyStruct + "\n" + valStruct
    }
    s"""namespace $namespace {
${metaMap.foldLeft("")( (acc, x) => acc + proc(x._2) + "\n" ).dropRight(1)}
}"""
  }
}


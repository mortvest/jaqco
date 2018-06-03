import CodeGenUtils._

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
      val code = s"""#ifndef JAQCO_SCHEMA_H
#define JAQCO_SCHEMA_H
#include "$typeDefFileName"
namespace $namespace {
  class ${schemaName}_schema_creator: public reactdb::abstract_schema_creator {
  public:
      void create_schema(reactdb::hypo_db* my_database) override {
${metaMap.foldLeft("")( (acc, x) => acc + "        " + genIndex(x._1, x._1 + "_key_type") + "\n" )}
      }
  }
  ;
}
#endif
"""
    taggify(code, "")
  }
  def genTypeDef(metaMap: Map[String, TableMetaData], namespace: String): String = {
    def swap(attName: String, attType: SimpleType) = {
      attType.typeName match {
          case "short" => s"__builtin_bswap16($attName)"
          case "int" => s"__builtin_bswap32($attName)"
          case "long" => s"__builtin_bswap64($attName)"
          case _ => throw new Error(s"Not supported yet")
        }
    }
    def encode(map: Map[String, DataType]) = {
      val buffSize = "auto sizeof_buf = " + map.keys.toList.foldLeft("")((acc, x) =>
        acc + s"sizeof(decltype($x)) + ").dropRight(3) + ";"
      def singleAttrib(attName: String, typeName: DataType) = typeName match {
        case simple: SimpleType => s"""
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  decltype(${attName}) swap_${attName} = ${swap(attName, simple)};
  std::memcpy(&buf[offset], &swap_${attName}, sizeof(decltype(${attName})));
#else
  std::memcpy(&buf[offset], &(${attName}), sizeof(decltype(${attName})));
#endif
  offset += sizeof(decltype(${attName}));
"""
        case StringType(len) => s"""
  std::memcpy(&buf[offset], &(${attName}), sizeof(decltype(${attName})));
  offset += sizeof(decltype(${attName}));
"""
      }
      s"""
inline std::string encode() {
  ${buffSize}
  std::size_t offset = 0;
  char buf[sizeof_buf];
  std::memset(&buf[0], 0, sizeof_buf);
${map.foldLeft("") ((acc, x) => acc + singleAttrib(x._1, x._2))}
  return std::string(buf, sizeof_buf);
}
"""
    }
    def decode(map: Map[String, DataType]) = {
      println(map)
      def singleAttrib(attName: String, typeName: DataType) = typeName match {
        case SimpleType(simple) => s"""
  std::memcpy(&${attName}, encoded_object.c_str() + offset, sizeof(decltype(${attName})));
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
  ${attName} = __builtin_bswap32(${attName});
#endif
  offset += sizeof(decltype(${attName}));
"""
        case StringType(len) => s"""
std::memcpy(&${attName}, encoded_object.c_str() + offset, sizeof(decltype(${attName})));
offset += sizeof(decltype(${attName}));
"""
      }
      s"""
inline void decode(const std::string& encoded_object) {
  std::size_t offset = 0;
${map.foldLeft("")((acc, x) => acc + singleAttrib(x._1, x._2))}
}
"""
    }
    def newKeyStruct(meta: TableMetaData, valType: String) = {
      val newMap = meta.attributes.filter { case x => meta.indexParts.contains(x._1) }
      val names = newMap.keys.toList
      val header = s"struct ${valType} {\n"
      val fields = newMap.foldLeft("") ((acc, x) => acc + "  " + structElem(x, " { 0 }") + "\n")
      val defConstr = s"  ${valType}() = default;\n"
      val altConstr = s"  ${valType}(" + names.foldLeft("")((acc, x) =>
        acc + s"decltype($x) my_${x}, ").dropRight(2) + ") : " + names.foldLeft("")((acc, x) =>
        acc + s"$x(my_$x), ").dropRight(2) + " {}\n"
      header + fields + "\n" + defConstr + altConstr + encode(newMap) + decode(newMap) + "};"
    }
    def proc(meta: TableMetaData) = {
      val attributes = meta.attributes
      val indexParts = meta.indexParts
      val name = meta.relName
      val keyStruct = newKeyStruct(meta, name+"_key_type")
      val valStruct = newValueStruct(meta, name+"_val_type")
      keyStruct + "\n" + valStruct
    }
    val code = s"""#ifndef JAQCO_TYPE_DEF_H
#define JAQCO_TYPE_DEF_H
namespace $namespace {
${metaMap.foldLeft("")( (acc, x) => acc + proc(x._2) + "\n" ).dropRight(1)}
}
#endif"""
    taggify(code, "")
  }
}


object DDLProcessor {
  def apply(meta: Map[String, TableMetaData] ): String = {
    def getType(name: String, map: Map[String, String]) = {
      (map get name) match {
        case Some(tp) => tp
        case None => throw new Error("Index part does not have a type")
      }
    }
    def genIndex(relName: String) = {
      ""
    }

    def proc(meta: TableMetaData) = {
      val attributes = meta.attributes
      val indexParts = meta.indexParts
      val name = meta.relName
      val keyStructName = name+"_key_type"
      val keyStruct = s"\nstruct ${keyStructName} {\n" + indexParts.foldLeft("") ( (acc, ex) => acc + getType(ex, attributes) + " " + ex + ";\n" ) + "}MACRO_PACKED;\n"
      val valStructName = name+"_val_type"
      val valStruct = s"\nstruct ${valStructName} {\n" +
      attributes.foldLeft("") ( (acc, ex) => acc + ex._2 + " " + ex._1 + ";\n" ) + "}MACRO_PACKED;\n"
      genIndex(name) + keyStruct + valStruct
    }
    meta.foldLeft("")( (acc, x) => acc + proc(x._2) )
  }
}

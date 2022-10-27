#ifndef ISEARCH_BS_TOOLS_DOCUMENT_PRINTER_H
#define ISEARCH_BS_TOOLS_DOCUMENT_PRINTER_H

#include <tr1/memory>
#include <tr1/functional>
#include <indexlib/indexlib.h>
#include <indexlib/document/index_document/normal_document/normal_document.h>
#include <indexlib/config/index_partition_schema.h>
#include "build_service/common_define.h"
#include "build_service/util/Log.h"

namespace build_service {
namespace tools {

class DocumentPrinter
{
public:
    DocumentPrinter();
    ~DocumentPrinter();

public:
    bool Init(const std::string& filter, const std::string& display,
              const std::string& schemaFile,
              const std::string& pkStr);
    bool PrintDocument(const IE_NAMESPACE(document)::NormalDocumentPtr& document,
                       std::stringstream& out) const;
    
private:
    std::string ConvertAttrValue(FieldType ft, const autil::ConstString& attrValue) const;
    
private:
    std::map<std::string, std::set<std::string>> mAttrFilter;
    IE_NAMESPACE(config)::IndexPartitionSchemaPtr mSchema;
    std::set<std::string> mDisplayAttrSet;
    std::string mPkStr;
private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(DocumentPrinter);

}
}

#endif //ISEARCH_BS_TOOLS_DOCUMENT_PRINTER_H


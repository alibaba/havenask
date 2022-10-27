#ifndef ISEARCH_BS_SUBDOCUMENTEXTRACTOR_H
#define ISEARCH_BS_SUBDOCUMENTEXTRACTOR_H

#include "build_service/common_define.h"
#include "build_service/util/Log.h"
#include <indexlib/config/index_partition_schema.h>
#include "build_service/document/RawDocument.h"
#include "build_service/document/RawDocumentHashMapManager.h"
namespace build_service {
namespace processor {

class SubDocumentExtractor
{
public:
    SubDocumentExtractor(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema);
    ~SubDocumentExtractor();

public:
    void extractSubDocuments(const document::RawDocumentPtr& originalDocument,
                             std::vector<document::RawDocumentPtr>& subDocuments);

private:
    void addFieldsToSubDocuments(const std::string& fieldName,
                                 const std::string& fieldValue,
                                 std::vector<document::RawDocumentPtr>& subDocuments,
                                 const document::RawDocumentPtr &originalDocument);

private:
    const IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;

private:
    BS_LOG_DECLARE();
};

BS_TYPEDEF_PTR(SubDocumentExtractor);

}
}

#endif //ISEARCH_BS_SUBDOCUMENTEXTRACTOR_H

#ifndef __INDEXLIB_SUB_DOCUMENT_EXTRACTOR_H
#define __INDEXLIB_SUB_DOCUMENT_EXTRACTOR_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/testlib/raw_document.h"

IE_NAMESPACE_BEGIN(testlib);

class SubDocumentExtractor
{
public:
    SubDocumentExtractor(const IE_NAMESPACE(config)::IndexPartitionSchemaPtr& schema);
    ~SubDocumentExtractor();

public:
    void extractSubDocuments(const RawDocumentPtr& originalDocument,
                             std::vector<RawDocumentPtr>& subDocuments);

private:
    void addFieldsToSubDocuments(const std::string& fieldName,
                                 const std::string& fieldValue,
                                 std::vector<RawDocumentPtr>& subDocuments);

private:
    const IE_NAMESPACE(config)::IndexPartitionSchemaPtr _schema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocumentExtractor);

IE_NAMESPACE_END(testlib);

#endif //__INDEXLIB_SUB_DOCUMENT_EXTRACTOR_H

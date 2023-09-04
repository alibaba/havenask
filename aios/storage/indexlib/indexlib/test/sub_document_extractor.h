#ifndef __INDEXLIB_SUB_DOCUMENT_EXTRACTOR_H
#define __INDEXLIB_SUB_DOCUMENT_EXTRACTOR_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/raw_document.h"

namespace indexlib { namespace test {

class SubDocumentExtractor
{
public:
    SubDocumentExtractor(const indexlib::config::IndexPartitionSchemaPtr& schema);
    ~SubDocumentExtractor();

public:
    void extractSubDocuments(const RawDocumentPtr& originalDocument, std::vector<RawDocumentPtr>& subDocuments);

private:
    void addFieldsToSubDocuments(const std::string& fieldName, const std::string& fieldValue,
                                 std::vector<RawDocumentPtr>& subDocuments);

private:
    const indexlib::config::IndexPartitionSchemaPtr _schema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SubDocumentExtractor);
}} // namespace indexlib::test

#endif //__INDEXLIB_SUB_DOCUMENT_EXTRACTOR_H

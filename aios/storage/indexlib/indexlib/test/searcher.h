#ifndef __INDEXLIB_SEARCHER_H
#define __INDEXLIB_SEARCHER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/config/attribute_schema.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/query.h"
#include "indexlib/test/raw_document.h"
#include "indexlib/test/result.h"
#include "indexlib/test/source_query.h"

DECLARE_REFERENCE_CLASS(partition, IndexPartitionReader);
DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);

namespace indexlib { namespace test {

class Searcher
{
public:
    Searcher();
    ~Searcher();

public:
    bool Init(const partition::IndexPartitionReaderPtr& reader, config::IndexPartitionSchema* schema);

    ResultPtr Search(QueryPtr query, TableSearchCacheType searchCacheType, std::string& errorInfo);
    ResultPtr Search(QueryPtr query, TableSearchCacheType searchCacheType);

private:
    void FillResult(QueryPtr query, docid_t docId, ResultPtr result, bool isDeleted);
    void FillRawDocBySource(SourceQueryPtr query, docid_t docId, RawDocumentPtr rawDoc);
    void FillPosition(QueryPtr query, RawDocumentPtr rawDoc);

    bool GetSingleAttributeValue(const config::AttributeSchemaPtr& attrSchema, const std::string& attrName,
                                 docid_t docId, std::string& attrValue);

private:
    partition::IndexPartitionReaderPtr mReader;
    config::IndexPartitionSchema* mSchema;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(Searcher);
}} // namespace indexlib::test

#endif //__INDEXLIB_SEARCHER_H

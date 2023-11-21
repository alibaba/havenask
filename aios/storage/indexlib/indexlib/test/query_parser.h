#pragma once

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common_define.h"
#include "indexlib/index/common/Term.h"
#include "indexlib/indexlib.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/test/query.h"

DECLARE_REFERENCE_CLASS(index, InvertedIndexReader);
DECLARE_REFERENCE_CLASS(config, IndexConfig);

namespace indexlib { namespace test {

static const std::string INDEX_TERM_SEPARATOR = ":";
static const std::string TERM_SEPARATOR = ",";
static const std::string DOCID = "__docid";
static const std::string SUBDOCID = "__subdocid";

class QueryParser
{
public:
    QueryParser();
    ~QueryParser();

public:
    static QueryPtr Parse(const std::string& queryString, partition::IndexPartitionReaderPtr reader,
                          autil::mem_pool::Pool* pool = nullptr);

private:
    static QueryPtr CreateDocidQuery(const std::string& indexName, const std::string& termString);
    static QueryPtr CreateSourceQuery(const std::string& groupIdString, const std::string& indexName,
                                      const std::string& termString, index::InvertedIndexReaderPtr reader,
                                      autil::mem_pool::Pool* pool);
    static QueryPtr CreateTermQuery(const std::string& indexName, const index::Term& term, PostingType type,
                                    index::InvertedIndexReaderPtr reader, autil::mem_pool::Pool* pool);
    static QueryPtr DoParse(const std::string& queryString, partition::IndexPartitionReaderPtr reader, bool isSubReader,
                            autil::mem_pool::Pool* pool);

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(QueryParser);
}} // namespace indexlib::test

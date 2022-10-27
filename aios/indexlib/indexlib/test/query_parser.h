#ifndef __INDEXLIB_QUERY_PARSER_H
#define __INDEXLIB_QUERY_PARSER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common/term.h"
#include "indexlib/common_define.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/test/query.h"

DECLARE_REFERENCE_CLASS(index, IndexReader);

IE_NAMESPACE_BEGIN(test);

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
    static QueryPtr Parse(const std::string& queryString,
                          partition::IndexPartitionReaderPtr reader);

private:
    static QueryPtr CreateDocidQuery(const std::string& indexName,
            const std::string& termString);
    static QueryPtr CreateTermQuery(const std::string& indexName,
                                    const common::Term& term,
                                    index::IndexReaderPtr reader);
    static QueryPtr DoParse(const std::string& queryString,
                            partition::IndexPartitionReaderPtr reader,
                            bool isSubReader = false);
    static bool ParseQueryStr(const std::string& dateRange,
                              int64_t& leftTimestamp,
                              int64_t& rightTimestamp);
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(QueryParser);

IE_NAMESPACE_END(test);

#endif //__INDEXLIB_QUERY_PARSER_H

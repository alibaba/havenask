#include "indexlib/test/query_parser.h"

#include "autil/StringUtil.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/config/date_index_config.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/index/common/NumberTerm.h"
#include "indexlib/index/inverted_index/InvertedIndexReader.h"
#include "indexlib/test/atomic_query.h"
#include "indexlib/test/docid_query.h"
#include "indexlib/test/or_query.h"
#include "indexlib/test/range_query_parser.h"
#include "indexlib/test/source_query.h"
#include "indexlib/util/TimestampUtil.h"

using namespace std;
using namespace autil;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index;
using namespace indexlib::util;
using namespace indexlib::partition;
using namespace indexlibv2::index;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, QueryParser);

QueryParser::QueryParser() {}

QueryParser::~QueryParser() {}

QueryPtr QueryParser::Parse(const std::string& queryString, IndexPartitionReaderPtr reader, mem_pool::Pool* pool)
{
    QueryPtr query = DoParse(queryString, reader, false, pool);
    if (query) {
        return query;
    }
    IndexPartitionReaderPtr subReader = reader->GetSubPartitionReader();
    if (subReader) {
        query = DoParse(queryString, subReader, true, pool);
        if (query) {
            query->SetSubQuery();
            return query;
        }
    }
    return QueryPtr();
}

namespace {
PostingType ParsePostingType(const std::string& str)
{
    if (str == "pt_normal") {
        return pt_normal;
    } else if (str == "pt_bitmap") {
        return pt_bitmap;
    } else {
        return pt_default;
    }
}

} // namespace

QueryPtr QueryParser::DoParse(const std::string& queryString, IndexPartitionReaderPtr reader, bool isSubReader,
                              mem_pool::Pool* pool)
{
    InvertedIndexReaderPtr indexReader = reader->GetInvertedIndexReader();
    assert(indexReader);
    vector<string> indexTerms = StringUtil::split(queryString, INDEX_TERM_SEPARATOR);
    assert(indexTerms.size() >= 2);
    string indexName = indexTerms[0];
    string termString = indexTerms[1];
    PostingType postingType = pt_default;
    if (indexTerms.size() > 2) {
        if (!termString.empty() && (termString[0] == '(' || termString[0] == '[')) {
            // time & date & timestamp, like"index:(1970-01-02 11:59:59,2020-01-01 00:00:00)"
            size_t pos = queryString.find(':') + 1;
            termString = queryString.substr(pos);
        } else {
            // group source, like "source_group:0:pk:1"
            assert(indexName == "source_group");
            assert(!indexTerms[0].empty() && indexTerms.size() == 4);
            return CreateSourceQuery(/*groupIdString*/ indexTerms[1], /*indexName*/ indexTerms[2],
                                     /*termString*/ indexTerms[3], /*reader*/ indexReader, /*pool*/ pool);
        }
    } else {
        assert(indexTerms.size() == 2);
    }
    auto pos = termString.find('$');
    if (pos != termString.npos) {
        auto postingTypeStr = termString.substr(pos + 1);
        termString = termString.substr(0, pos);
        postingType = ParsePostingType(postingTypeStr);
    }
    auto schema = reader->GetSchema();
    assert(schema);
    if ((!isSubReader && indexName == DOCID) || (isSubReader && indexName == SUBDOCID)) {
        return CreateDocidQuery(indexName, termString);
    }
    auto indexSchema = schema->GetIndexSchema();
    config::IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    if (termString == "IS_NULL" ||
        (indexConfig && indexConfig->SupportNull() && indexConfig->GetNullTermLiteralString() == termString)) {
        // lookup null term
        Term term(indexName);
        return CreateTermQuery(indexName, term, postingType, indexReader, pool);
    }

    if (indexConfig &&
        (it_datetime == indexConfig->GetInvertedIndexType() || it_range == indexConfig->GetInvertedIndexType())) {
        int64_t leftNumber, rightNumber;
        if (!RangeQueryParser::ParseQueryStr(indexConfig, termString, leftNumber, rightNumber)) {
            return QueryPtr();
        }
        auto singleIndexConfig = DYNAMIC_POINTER_CAST(config::SingleFieldIndexConfig, indexConfig);
        if (RangeFieldEncoder::UINT ==
                RangeFieldEncoder::GetRangeFieldType(singleIndexConfig->GetFieldConfig()->GetFieldType()) &&
            leftNumber < 0) {
            leftNumber = 0;
        }
        index::Int64Term term(leftNumber, true, rightNumber, true, indexName);
        return CreateTermQuery(indexName, term, postingType, indexReader, pool);
    }

    Term term(termString, indexName);
    return CreateTermQuery(indexName, term, postingType, indexReader, pool);
}

QueryPtr QueryParser::CreateSourceQuery(const string& groupIdString, const string& indexName, const string& termString,
                                        InvertedIndexReaderPtr reader, mem_pool::Pool* pool)
{
    sourcegroupid_t groupId;
    if (!StringUtil::fromString(groupIdString, groupId)) {
        IE_LOG(ERROR, "given groupId[%s] should be int32 number", groupIdString.c_str());
        return QueryPtr();
    }
    if (groupId < 0) {
        IE_LOG(ERROR,
               "given groupId[%s] which convertor to [%d]"
               "should be non-negative number",
               groupIdString.c_str(), groupId);
        return QueryPtr();
    }
    Term term(termString, indexName);
    PostingIterator* iter = reader->Lookup(term, 1000, pt_default, pool).Value();
    if (!iter) {
        return QueryPtr();
    }
    SourceQueryPtr query(new SourceQuery);
    query->Init(iter, indexName, groupId);
    return query;
}

QueryPtr QueryParser::CreateDocidQuery(const string& indexName, const string& termString)
{
    DocidQueryPtr query(new DocidQuery);
    query->Init(termString);
    return query;
}

QueryPtr QueryParser::CreateTermQuery(const string& indexName, const Term& term, PostingType type,
                                      index::InvertedIndexReaderPtr reader, mem_pool::Pool* pool)
{
    PostingIterator* iter = reader->Lookup(term, 1000, type, pool).Value();
    if (!iter) {
        return QueryPtr();
    }
    AtomicQueryPtr query(new AtomicQuery);
    query->Init(iter, indexName);
    query->SetWord(term.GetWord());
    return query;
}
}} // namespace indexlib::test

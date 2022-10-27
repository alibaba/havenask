#include <autil/StringUtil.h>
#include "indexlib/test/query_parser.h"
#include "indexlib/test/atomic_query.h"
#include "indexlib/test/or_query.h"
#include "indexlib/test/docid_query.h"
#include "indexlib/common/number_term.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/config/range_index_config.h"
#include "indexlib/config/impl/range_index_config_impl.h"
#include "indexlib/common/field_format/range/range_field_encoder.h"
#include "indexlib/index/normal/inverted_index/accessor/multi_field_index_reader.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, QueryParser);

QueryParser::QueryParser() 
{
}

QueryParser::~QueryParser() 
{
}

QueryPtr QueryParser::Parse(const std::string& queryString,
                            IndexPartitionReaderPtr reader)
{
    QueryPtr query = DoParse(queryString, reader);
    if (query)
    {
        return query;
    }
    IndexPartitionReaderPtr subReader = reader->GetSubPartitionReader();
    if (subReader)
    {
        query = DoParse(queryString, subReader, true);
        if (query)
        {
            query->SetSubQuery();
            return query;
        }
    }
    return QueryPtr();
}

QueryPtr QueryParser::DoParse(const std::string& queryString,
                              IndexPartitionReaderPtr reader,
                              bool isSubReader)
{
    vector<string> indexTerms = StringUtil::split(queryString, INDEX_TERM_SEPARATOR);
    string indexName = indexTerms[0];
    string termString = indexTerms[1];
    assert(indexTerms.size() == 2);
    IndexReaderPtr indexReaderPtr = reader->GetIndexReader();
    auto schema = reader->GetSchema();
    assert(indexReaderPtr);
    assert(schema);
    if ((!isSubReader && indexName == DOCID)
        || (isSubReader && indexName == SUBDOCID))
    {        
        return CreateDocidQuery(indexName, termString);
    }
    auto indexSchema = schema->GetIndexSchema();
    config::IndexConfigPtr indexConfig = indexSchema->GetIndexConfig(indexName);
    if (indexConfig &&
        (it_date == indexConfig->GetIndexType()
         || it_range == indexConfig->GetIndexType()))
    {
        int64_t leftNumber, rightNumber;
        if (!ParseQueryStr(termString, leftNumber, rightNumber))
        {
            return QueryPtr();
        }
        auto singleIndexConfig = DYNAMIC_POINTER_CAST(config::SingleFieldIndexConfig,
                indexConfig);
        if (RangeFieldEncoder::UINT == RangeFieldEncoder::GetRangeFieldType(
                        singleIndexConfig->GetFieldConfig()->GetFieldType())
            && leftNumber < 0)
        {
            leftNumber = 0;
        }
        Int64Term term(leftNumber, true, rightNumber, true, indexName);
        return CreateTermQuery(indexName, term, indexReaderPtr);
    }
    Term term(termString, indexName);
    return CreateTermQuery(indexName, term, indexReaderPtr);
}

QueryPtr QueryParser::CreateDocidQuery(const string& indexName,
                                      const string& termString)
{
    DocidQueryPtr query(new DocidQuery);
    query->Init(termString);
    return query;
}

bool QueryParser::ParseQueryStr(const std::string& dateRange,
                                    int64_t& leftTimestamp,
                                    int64_t& rightTimestamp)
{
    if (dateRange.empty())
    {
        return false;
    }

    std::vector<std::string> strVec = StringUtil::split(dateRange, ",");
    if ((int)strVec.size() != 2 || strVec[0].length() < 1 || strVec[1].length() < 1)
    {
        return false;
    }

    if (strVec[0][0] != '(' && strVec[0][0] != '[')
    {
        return false;
    }

    if (strVec[1].back() != ')' && strVec[1].back() != ']')
    {
        return false;
    }
    int rightNumberLength = (int)strVec[1].length() - 1;
    bool isLeftClose = strVec[0][0] == '[';    
    bool isRightClose = strVec[1].back() == ']';
    if (strVec[0].length() == 1 && strVec[0][0] == '(')
    {
        leftTimestamp = std::numeric_limits<int64_t>::min();
        isLeftClose = true;
    }
    else if (!StringUtil::numberFromString(strVec[0].substr(1), leftTimestamp))
    {
        return false;
    }
    if (strVec[1].length() == 1 && strVec[1][0] == ')')
    {
        rightTimestamp = std::numeric_limits<int64_t>::max();
        isRightClose = true;
    }
    else if (!StringUtil::numberFromString(strVec[1].substr(0, rightNumberLength), rightTimestamp))
    {
        return false;
    }
    if (!isLeftClose)
        leftTimestamp ++;
    if (!isRightClose)
        rightTimestamp --;

    return leftTimestamp <= rightTimestamp;
}
    
QueryPtr QueryParser::CreateTermQuery(const string& indexName, const Term& term,
                                      index::IndexReaderPtr reader)
{
    PostingIterator* iter = reader->Lookup(term);
    if (!iter)
    {
        return QueryPtr();
    }
    AtomicQueryPtr query(new AtomicQuery);
    query->Init(iter, indexName);
    return query;
}

IE_NAMESPACE_END(test);


#include "indexlib/test/query.h"

using namespace std;

IE_NAMESPACE_BEGIN(test);
IE_LOG_SETUP(test, Query);

Query::Query(QueryType queryType) 
    : mIsSubQuery(false)
    , mQueryType(queryType)
{
}

Query::~Query() 
{
}

IE_NAMESPACE_END(test);


#include "indexlib/test/query.h"

using namespace std;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, Query);

Query::Query(QueryType queryType) : mIsSubQuery(false), mQueryType(queryType) {}

Query::~Query() {}
}} // namespace indexlib::test

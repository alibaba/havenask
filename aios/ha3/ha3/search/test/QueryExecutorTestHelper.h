#ifndef ISEARCH_QUERYEXECUTORTESTHELPER_H
#define ISEARCH_QUERYEXECUTORTESTHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3_sdk/testlib/index/FakeIndexPartitionReaderCreator.h>

BEGIN_HA3_NAMESPACE(search);

class QueryExecutorTestHelper
{
public:
    QueryExecutorTestHelper();
    ~QueryExecutorTestHelper();
private:
    QueryExecutorTestHelper(const QueryExecutorTestHelper &);
    QueryExecutorTestHelper& operator=(const QueryExecutorTestHelper &);
public:
    static void addSubDocAttrMap(index::FakeIndex &fakeIndex, const std::string &subDocAttrMap);

    static void addSubDocAttrMap(index::FakeIndex &fakeIndex,
                                 const std::string &mainToSubAttrMap,
                                 const std::string &subToMainAttrMap);
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(QueryExecutorTestHelper);

END_HA3_NAMESPACE(search);

#endif //ISEARCH_QUERYEXECUTORTESTHELPER_H

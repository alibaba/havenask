#ifndef ISEARCH_PHRASEQUERYEXECUTORTEST_H
#define ISEARCH_PHRASEQUERYEXECUTORTEST_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/search/QueryExecutor.h>
#include <ha3/rank/MatchData.h>
#include <ha3_sdk/testlib/index/FakeTextIndexReader.h>
#include <indexlib/index/normal/inverted_index/accessor/index_reader.h>
#include <ha3/search/IndexPartitionReaderWrapper.h>

BEGIN_HA3_NAMESPACE(search);

class PhraseQueryExecutorTest : public TESTBASE {
public:
    PhraseQueryExecutorTest();
    ~PhraseQueryExecutorTest();
public:
    void setUp();
    void tearDown();
protected:
    void createIndexPartitionReaderWrapper(
            const std::string &indexStr, 
            const index::FakeTextIndexReader::PositionMap &posMap = index::FakeTextIndexReader::PositionMap());
    QueryExecutor *_queryExecutor;
    autil::mem_pool::Pool *_pool;
    IndexPartitionReaderWrapperPtr _indexPartitionReaderWrapper;
private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(search);

#endif //ISEARCH_PHRASEQUERYEXECUTORTEST_H

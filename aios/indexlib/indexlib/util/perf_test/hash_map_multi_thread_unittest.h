#ifndef __INDEXLIB_HASHMAPMULTITHREADPERFTEST_H
#define __INDEXLIB_HASHMAPMULTITHREADPERFTEST_H

#include <pthread.h>
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/util/hash_map.h"

IE_NAMESPACE_BEGIN(util);

class HashMapMultiThreadPerfTest : public INDEXLIB_TESTBASE {
public:
    HashMapMultiThreadPerfTest();
    ~HashMapMultiThreadPerfTest();

public:
    void SetUp();
    void TearDown();
    void TestHashMapMultiThread();

private:
    void DoTestHashMapMultiThread(size_t threadNum, bool *status);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(HashMapMultiThreadPerfTest, TestHashMapMultiThread);

IE_NAMESPACE_END(util);

#endif //__INDEXLIB_HASHMAPMULTITHREADPERFTEST_H

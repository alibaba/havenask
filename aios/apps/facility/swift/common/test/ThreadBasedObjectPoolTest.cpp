#include "swift/common/ThreadBasedObjectPool.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <pthread.h>
#include <stdint.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/Thread.h"
#include "autil/TimeUtility.h"
#include "ext/alloc_traits.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil;

namespace swift {
namespace common {

class ThreadBasedObjectPoolTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

private:
    static void threadFunction(ThreadBasedObjectPool<string> *object);
    static void threadFunction2(ThreadBasedObjectPool<string> *object);
};

void ThreadBasedObjectPoolTest::setUp() {}

void ThreadBasedObjectPoolTest::tearDown() {}

string *create() { return new string("123"); }

TEST_F(ThreadBasedObjectPoolTest, testGetObjectWithMutliThreadWithCreateFunc) {
    ThreadBasedObjectPool<string> factory(std::bind(&create));
    ASSERT_TRUE(factory._objectMap.empty());
    int32_t threadNum = 100;
    vector<ThreadPtr> threadVec;
    vector<pthread_t> threadIdVec;
    for (int32_t i = 0; i < threadNum; ++i) {
        ThreadPtr threadPtr = Thread::createThread(bind(&ThreadBasedObjectPoolTest::threadFunction2, &factory));
        threadVec.push_back(threadPtr);
        threadIdVec.push_back(threadPtr->getId());
    }
    for (int32_t i = 0; i < threadNum; ++i) {
        threadVec[i]->join();
    }
    ASSERT_EQ((size_t)threadNum, factory._objectMap.size());
    for (int32_t i = 0; i < threadNum; ++i) {
        typename ThreadBasedObjectPool<string>::ObjectMap::iterator it = factory._objectMap.find(threadIdVec[i]);
        ASSERT_TRUE(factory._objectMap.end() != it);
        ASSERT_TRUE(NULL != it->second);
    }
}

TEST_F(ThreadBasedObjectPoolTest, testGetObjectWithMutliThread) {
    ThreadBasedObjectPool<string> factory;
    ASSERT_TRUE(factory._objectMap.empty());
    int32_t threadNum = 100;
    vector<ThreadPtr> threadVec;
    vector<pthread_t> threadIdVec;
    for (int32_t i = 0; i < threadNum; ++i) {
        ThreadPtr threadPtr = Thread::createThread(bind(&ThreadBasedObjectPoolTest::threadFunction, &factory));
        threadVec.push_back(threadPtr);
        threadIdVec.push_back(threadPtr->getId());
    }
    for (int32_t i = 0; i < threadNum; ++i) {
        threadVec[i]->join();
    }
    ASSERT_EQ((size_t)threadNum, factory._objectMap.size());
    for (int32_t i = 0; i < threadNum; ++i) {
        typename ThreadBasedObjectPool<string>::ObjectMap::iterator it = factory._objectMap.find(threadIdVec[i]);
        ASSERT_TRUE(factory._objectMap.end() != it);
        ASSERT_TRUE(NULL != it->second);
    }
}

void ThreadBasedObjectPoolTest::threadFunction(ThreadBasedObjectPool<string> *object) {
    for (int i = 0; i < 10000; i++) {
        if (i % 2 == 0) {
            *object->getObject() = StringUtil::toString(i);
        } else {
            ASSERT_EQ(StringUtil::toString(i - 1), *object->getObject());
        }
    }
}

void ThreadBasedObjectPoolTest::threadFunction2(ThreadBasedObjectPool<string> *object) {
    for (int i = 0; i < 10000; i++) {
        ASSERT_EQ(string("123"), *object->getObject());
    }
}

}; // namespace common
}; // namespace swift

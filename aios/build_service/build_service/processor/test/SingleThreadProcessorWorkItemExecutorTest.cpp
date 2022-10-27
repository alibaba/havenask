#include "build_service/test/unittest.h"
#include "build_service/processor/SingleThreadProcessorWorkItemExecutor.h"
#include "build_service/processor/ProcessorWorkItem.h"

using namespace std;
using namespace testing;

namespace build_service {
namespace processor {

class SingleThreadProcessorWorkItemExecutorTest : public BUILD_SERVICE_TESTBASE {
public:
    void setUp();
    void tearDown();
};

class TestWorkItem : public ProcessorWorkItem {
public:
    TestWorkItem(int id,  int* processFlagVec, int* deleteFlagVec)
        : ProcessorWorkItem(DocumentProcessorChainVecPtr(), ProcessorChainSelectorPtr(), NULL)
        , _id(id)
        , _processFlagVec(processFlagVec)
        , _deleteFlagVec(deleteFlagVec)
    {
        _deleteFlagVec[_id] = 0;
        _processFlagVec[_id] = 0;        
    }
    
    ~TestWorkItem() {
        _deleteFlagVec[_id] = 1;
    }
    
public:
    /* override */ void process() {
        _processFlagVec[_id] = 1;
    }

private:
    int _id;
    int* _processFlagVec;
    int* _deleteFlagVec;
};
    
void SingleThreadProcessorWorkItemExecutorTest::setUp() {
}

void SingleThreadProcessorWorkItemExecutorTest::tearDown() {
}

TEST_F(SingleThreadProcessorWorkItemExecutorTest, testSimple) {
    uint32_t queueSize = 10;
    int* deleteFlagVec = new int[queueSize];
    int* processFlagVec = new int[queueSize];

    {
        SingleThreadProcessorWorkItemExecutor executor(queueSize);
        ASSERT_FALSE(executor._running);
        ProcessorWorkItem* item = new TestWorkItem(0, processFlagVec, deleteFlagVec);
        ASSERT_FALSE(executor.push(item));
        delete item;

        executor.start();
        ASSERT_TRUE(executor._running); 
        ASSERT_FALSE(executor.push(NULL));

        for (size_t i = 0; i < queueSize; ++i) {
            item = new TestWorkItem(i, processFlagVec, deleteFlagVec);
            ASSERT_TRUE(executor.push(item));
            ASSERT_EQ(1, processFlagVec[i]);
            ASSERT_EQ(uint32_t(0), executor.getWaitItemCount());
            ASSERT_EQ(uint32_t(i+1), executor.getOutputItemCount()); 
        }

        ASSERT_EQ(queueSize, executor._queue->size());


        size_t popItemCount = queueSize / 2;
        for (size_t i = 1; i <= popItemCount; ++i) {
            item = executor.pop();
            ASSERT_TRUE(item);
            ASSERT_EQ(uint32_t(0), executor.getWaitItemCount());
            ASSERT_EQ(uint32_t(queueSize - i), executor.getOutputItemCount());
            delete item;
        }

        executor.stop();
        ASSERT_FALSE(executor._running);
    }

    for (size_t i = 0; i < queueSize; ++i) {
        ASSERT_EQ(1, deleteFlagVec[i]);
    }
    
    delete [] deleteFlagVec;
    delete [] processFlagVec;
}

}
}

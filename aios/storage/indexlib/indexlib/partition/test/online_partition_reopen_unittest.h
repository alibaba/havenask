#ifndef __INDEXLIB_ONLINEPARTITIONREOPENTEST_H
#define __INDEXLIB_ONLINEPARTITIONREOPENTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/online_partition.h"
#include "indexlib/test/partition_state_machine.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace partition {

class OnlinePartitionReopenTest : public INDEXLIB_TESTBASE
{
public:
    OnlinePartitionReopenTest();
    ~OnlinePartitionReopenTest();

    DECLARE_CLASS_NAME(OnlinePartitionReopenTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestReopen();
    void TestForceReopen();
    void TestSimpleProcess();
    void TestAsyncDumpReopenFail();
    void TestAsyncDumpReopenSuccess_1();
    void TestAsyncDumpReopenSuccess_2();
    void TestForceOpen();
    void TestAsyncDumpForceOpen();
    void TestOptimizedReopenRandom();
    void TestOptimizedReopenWithAsyncDump();
    void TestOptimizedAsyncDumpWithRedo();

protected:
    void DoTest(std::function<void(int64_t, int64_t, bool&, bool&)> tester);
    void InnerTestReopen(int64_t triggerErrorIdx, int64_t continueFailIdx, bool& firstErrorTriggered,
                         bool& secondErrorTriggered);
    void InnerTestForceReopen(int64_t triggerErrorIdx, int64_t continueFailIdx, bool& firstErrorTriggered,
                              bool& secondErrorTriggered);
    void InnerTestRollBackWithoutRT(int64_t triggerErrorIdx);

    void InnerTestKVReopen(int64_t triggerErrorIdx, int64_t continueFailIdx, bool& firstErrorTriggered,
                           bool& secondErrorTriggered);

    void InnerTestKVForceReopen(int64_t triggerErrorIdx, int64_t continueFailIdx, bool& firstErrorTriggered,
                                bool& secondErrorTriggered);
    void InnerTestAsyncDumpReopen(size_t triggerErrorIdx, const std::string& incDoc, const std::string& reopenQuery,
                                  const std::string& reopenResult);
    void InnerTestForceOpen(int64_t triggerErrorIdx, int64_t continueFailIdx, bool& firstErrorTriggered,
                            bool& secondErrorTriggered);

    void rtBuild(std::map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>>* updateKey,
                 test::PartitionStateMachine* psm, const IndexBuilderPtr& builder, int64_t* time,
                 autil::ThreadMutex* dataLock, autil::ThreadMutex* forceReopenLock);
    void offlineBuild(std::map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>>* updateKey,
                      std::map<uint64_t, uint64_t> offlineBuildKey, test::PartitionStateMachine* psm,
                      int64_t* offlineBuildDoc, int64_t* time, autil::ThreadMutex* dataLock,
                      autil::ThreadMutex* forceReopenLock);
    void query(const OnlinePartition* onlinePartition, const config::IndexPartitionSchemaPtr& schema,
               std::map<uint64_t, std::vector<std::pair<uint64_t, uint64_t>>>* updateKey, autil::ThreadMutex* dataLock,
               autil::ThreadMutex* forceReopenLock);
    bool checkResult(const std::string& key, test::ResultPtr result, test::ResultPtr expectResult);
    void InnerTestForOptimizeReopenRandom(bool enableAsyncDump, bool enableOptimizedReopen);

protected:
    static const size_t NO_ERROR = 100000000;
    config::IndexPartitionSchemaPtr mSchema;
    std::string mFullDocs;
    std::string mIncDocs;
    std::string mRtDocs;
    std::string mRtDocs1;
    std::string mRtDocs2;

    std::string mKVFullDocs;
    std::string mKVRtDocs0;
    std::string mKVRtDocs1;
    std::string mKVIncDocs;
    std::string mKVRtDocs2;

private:
    IE_LOG_DECLARE();
};
}} // namespace indexlib::partition

#endif //__INDEXLIB_ONLINEPARTITIONREOPENTEST_H

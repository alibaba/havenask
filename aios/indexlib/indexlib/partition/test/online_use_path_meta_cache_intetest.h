#ifndef __INDEXLIB_ONLINEUSEPATHMETACACHETEST_H
#define __INDEXLIB_ONLINEUSEPATHMETACACHETEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/index_partition_options.h"
#include "indexlib/test/partition_state_machine.h"

DECLARE_REFERENCE_CLASS(file_system, Directory);
IE_NAMESPACE_BEGIN(partition);

enum OnlineUsePathMetaCacheTest_OnlinePartitionEvent
{
    OPE_OPEN,
    OPE_NORMAL_REOPEN,
    OPE_FORCE_REOPEN
};

class OnlineUsePathMetaCacheTest : public INDEXLIB_TESTBASE_WITH_PARAM<OnlineUsePathMetaCacheTest_OnlinePartitionEvent>
{
public:
    OnlineUsePathMetaCacheTest();
    ~OnlineUsePathMetaCacheTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestIndexPartialRemote();
    void TestIndexAllRemote();
    void TestIndexAllLocal();
    void TestPackageIndexPartialRemote();
    
private:
    void InitDocs();
    void CheckDocs(test::PartitionStateMachine& psm);
    void CheckPathMetaCache(test::PartitionStateMachine& psm,
                            OnlineUsePathMetaCacheTest_OnlinePartitionEvent event,
                            std::string dpToLocalFile);

private:
    std::string mPrimaryPath;
    std::string mSecondaryPath;
    std::string mFullDocs, mIncDocs1, mIncDocs2;
    config::IndexPartitionOptions mOptions;
    config::IndexPartitionSchemaPtr mSchema;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
    OnlineUsePathMetaCacheTestMode, OnlineUsePathMetaCacheTest,
    testing::Values(OnlineUsePathMetaCacheTest_OnlinePartitionEvent::OPE_OPEN,
                    OnlineUsePathMetaCacheTest_OnlinePartitionEvent::OPE_NORMAL_REOPEN,
                    OnlineUsePathMetaCacheTest_OnlinePartitionEvent::OPE_FORCE_REOPEN));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineUsePathMetaCacheTestMode, TestIndexPartialRemote);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineUsePathMetaCacheTestMode, TestIndexAllRemote);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineUsePathMetaCacheTestMode, TestIndexAllLocal);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(OnlineUsePathMetaCacheTestMode, TestPackageIndexPartialRemote);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_ONLINEUSEPATHMETACACHETEST_H

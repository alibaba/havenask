#ifndef __INDEXLIB_REOPENDECIDERTEST_H
#define __INDEXLIB_REOPENDECIDERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/reopen_decider.h"

IE_NAMESPACE_BEGIN(partition);

class ReopenDeciderTest : public INDEXLIB_TESTBASE
{
public:
    ReopenDeciderTest();
    ~ReopenDeciderTest();

    DECLARE_CLASS_NAME(ReopenDeciderTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNoNeedReopen();
    void TestNeedReclaimReaderReopen();
    void TestNeedSwitchFlushRtSegments();
    void TestNormalReopen();
    void TestForceReopen();
    void TestReopenWithNewSchemaVersion();

private:
    void InnerTestForNoNewIncVersion(
            const config::OnlineConfig& onlineConfig,
            const index::AttributeMetrics& attrMetrics,
            bool forceReopen,
            ReopenDecider::ReopenType expectType);

    void InnerTestForNewIncVersion(bool forceReopen,
                                   bool newSchemaVersion,
                                   ReopenDecider::ReopenType expectType);

private:
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionSchemaPtr mRtSchema;
    std::string mRootDir;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(ReopenDeciderTest, TestNoNeedReopen);
INDEXLIB_UNIT_TEST_CASE(ReopenDeciderTest, TestNeedReclaimReaderReopen);
INDEXLIB_UNIT_TEST_CASE(ReopenDeciderTest, TestNormalReopen);
INDEXLIB_UNIT_TEST_CASE(ReopenDeciderTest, TestForceReopen);
INDEXLIB_UNIT_TEST_CASE(ReopenDeciderTest, TestNeedSwitchFlushRtSegments);
INDEXLIB_UNIT_TEST_CASE(ReopenDeciderTest, TestReopenWithNewSchemaVersion);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_REOPENDECIDERTEST_H

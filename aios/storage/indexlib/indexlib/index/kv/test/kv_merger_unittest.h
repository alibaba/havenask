#ifndef __INDEXLIB_KVMERGERTEST_H
#define __INDEXLIB_KVMERGERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KvMergerTest : public INDEXLIB_TESTBASE
{
public:
    KvMergerTest();
    ~KvMergerTest();

    DECLARE_CLASS_NAME(KvMergerTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestInit();
    void TestNeedCompactBucket();

private:
    void RewriteConfig(config::KVIndexConfigPtr kvConfig, bool isLegacy, int32_t ttl, bool useCompactValue);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KvMergerTest, TestInit);
INDEXLIB_UNIT_TEST_CASE(KvMergerTest, TestNeedCompactBucket);
}} // namespace indexlib::index

#endif //__INDEXLIB_KVMERGERTEST_H

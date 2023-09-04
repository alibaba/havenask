#ifndef __INDEXLIB_KVDOCTIMESTAMPDECIDERTEST_H
#define __INDEXLIB_KVDOCTIMESTAMPDECIDERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/build_config.h"
#include "indexlib/config/offline_config.h"
#include "indexlib/document/document.h"
#include "indexlib/index/kv/kv_doc_timestamp_decider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class KvDocTimestampDeciderTest : public INDEXLIB_TESTBASE
{
public:
    KvDocTimestampDeciderTest();
    ~KvDocTimestampDeciderTest();

    DECLARE_CLASS_NAME(KvDocTimestampDeciderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(KvDocTimestampDeciderTest, TestSimpleProcess);
}} // namespace indexlib::index

#endif //__INDEXLIB_KVDOCTIMESTAMPDECIDERTEST_H

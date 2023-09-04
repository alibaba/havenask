#ifndef __INDEXLIB_PRIMARYKEYFORMATTERCREATORTEST_H
#define __INDEXLIB_PRIMARYKEYFORMATTERCREATORTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter_creator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class PrimaryKeyFormatterCreatorTest : public INDEXLIB_TESTBASE
{
public:
    PrimaryKeyFormatterCreatorTest();
    ~PrimaryKeyFormatterCreatorTest();

    DECLARE_CLASS_NAME(PrimaryKeyFormatterCreatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCreate();

private:
    config::SingleFieldIndexConfigPtr mPkConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyFormatterCreatorTest, TestCreate);
}} // namespace indexlib::index

#endif //__INDEXLIB_PRIMARYKEYFORMATTERCREATORTEST_H

#ifndef __INDEXLIB_PRIMARYKEYFORMATTERCREATORTEST_H
#define __INDEXLIB_PRIMARYKEYFORMATTERCREATORTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/primarykey/primary_key_formatter_creator.h"
#include "indexlib/config/single_field_index_config.h"
#include "indexlib/config/impl/single_field_index_config_impl.h"

IE_NAMESPACE_BEGIN(index);

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

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_PRIMARYKEYFORMATTERCREATORTEST_H

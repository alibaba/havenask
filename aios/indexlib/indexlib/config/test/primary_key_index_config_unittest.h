#ifndef __INDEXLIB_PRIMARYKEYINDEXCONFIGTEST_H
#define __INDEXLIB_PRIMARYKEYINDEXCONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/config/impl/primary_key_index_config_impl.h"

IE_NAMESPACE_BEGIN(config);

class PrimaryKeyIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    PrimaryKeyIndexConfigTest();
    ~PrimaryKeyIndexConfigTest();

    DECLARE_CLASS_NAME(PrimaryKeyIndexConfigTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCheckPrimaryKey();
    void TestJsonizePkIndexType();

private:
    void CheckJsonizePkIndexType(const std::string& jsonStr,
                                 PrimaryKeyIndexType expectType,
                                 bool isExpectException);
    
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexConfigTest, TestCheckPrimaryKey);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexConfigTest, TestJsonizePkIndexType);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PRIMARYKEYINDEXCONFIGTEST_H

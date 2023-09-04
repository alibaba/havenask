#ifndef __INDEXLIB_PRIMARYKEYINDEXCONFIGTEST_H
#define __INDEXLIB_PRIMARYKEYINDEXCONFIGTEST_H

#include "indexlib/common_define.h"
#include "indexlib/config/primary_key_index_config.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace config {

class PrimaryKeyIndexConfigTest : public INDEXLIB_TESTBASE
{
public:
    PrimaryKeyIndexConfigTest();
    ~PrimaryKeyIndexConfigTest();

    DECLARE_CLASS_NAME(PrimaryKeyIndexConfigTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimple();
    void TestCheckPrimaryKey();
    void TestJsonizePkIndexType();
    void TestSupportNullException();

private:
    void CheckJsonizePkIndexType(const std::string& jsonStr, PrimaryKeyIndexType expectType, bool isExpectException);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexConfigTest, TestSimple);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexConfigTest, TestCheckPrimaryKey);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexConfigTest, TestJsonizePkIndexType);
INDEXLIB_UNIT_TEST_CASE(PrimaryKeyIndexConfigTest, TestSupportNullException);
}} // namespace indexlib::config

#endif //__INDEXLIB_PRIMARYKEYINDEXCONFIGTEST_H

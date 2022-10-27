#ifndef __INDEXLIB_LOADCONFIGTEST_H
#define __INDEXLIB_LOADCONFIGTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/file_system/load_config/load_config.h"

IE_NAMESPACE_BEGIN(file_system);

class LoadConfigTest : public INDEXLIB_TESTBASE {
public:
    LoadConfigTest();
    ~LoadConfigTest();

    DECLARE_CLASS_NAME(LoadConfigTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestEqual();
    void TestDefaultValue();
    void TestRegularExpressionParse();
    void TestMatch();
    void TestMatchBuiltInPattern();
    void TestMatchBuiltInPatternWithLevel();
    void TestMatchBuiltInPatternWithSub();
    void TestMatchBuiltInPatternWithSubWithLevel();
    void TestBuiltInPatternToRegexPattern();

private:
    bool DoTestMatch(const std::string& pattern, const std::string& path);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(LoadConfigTest, TestEqual);
INDEXLIB_UNIT_TEST_CASE(LoadConfigTest, TestDefaultValue);
INDEXLIB_UNIT_TEST_CASE(LoadConfigTest, TestRegularExpressionParse);
INDEXLIB_UNIT_TEST_CASE(LoadConfigTest, TestMatch);
INDEXLIB_UNIT_TEST_CASE(LoadConfigTest, TestMatchBuiltInPattern);
INDEXLIB_UNIT_TEST_CASE(LoadConfigTest, TestMatchBuiltInPatternWithLevel);
INDEXLIB_UNIT_TEST_CASE(LoadConfigTest, TestMatchBuiltInPatternWithSub);
INDEXLIB_UNIT_TEST_CASE(LoadConfigTest, TestMatchBuiltInPatternWithSubWithLevel);
INDEXLIB_UNIT_TEST_CASE(LoadConfigTest, TestBuiltInPatternToRegexPattern);

IE_NAMESPACE_END(file_system);

#endif //__INDEXLIB_LOADCONFIGTEST_H

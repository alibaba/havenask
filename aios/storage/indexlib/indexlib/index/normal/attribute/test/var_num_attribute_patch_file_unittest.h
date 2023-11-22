#pragma once

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_file.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class VarNumAttributePatchFileTest : public INDEXLIB_TESTBASE
{
public:
    VarNumAttributePatchFileTest();
    ~VarNumAttributePatchFileTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestReadAndMove();
    void TestGetPatchValue();
    void TestGetPatchValueForNull();
    void TestSkipCurDocValue();
    void TestGetPatchValueForMultiChar();
    void TestSkipCurDocValueForMultiChar();

private:
    std::string mRootDir;
    config::AttributeConfigPtr mAttrConfig;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestReadAndMove);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestGetPatchValue);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestGetPatchValueForNull);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestSkipCurDocValue);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestGetPatchValueForMultiChar);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestSkipCurDocValueForMultiChar);
}} // namespace indexlib::index

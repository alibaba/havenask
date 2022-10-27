#ifndef __INDEXLIB_VARNUMATTRIBUTEPATCHFILETEST_H
#define __INDEXLIB_VARNUMATTRIBUTEPATCHFILETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_file.h"

IE_NAMESPACE_BEGIN(index);

class VarNumAttributePatchFileTest : public INDEXLIB_TESTBASE {
public:
    VarNumAttributePatchFileTest();
    ~VarNumAttributePatchFileTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestReadAndMove();
    void TestGetPatchValue();
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
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestSkipCurDocValue);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestGetPatchValueForMultiChar);
INDEXLIB_UNIT_TEST_CASE(VarNumAttributePatchFileTest, TestSkipCurDocValueForMultiChar);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VARNUMATTRIBUTEPATCHFILETEST_H

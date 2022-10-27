#ifndef __INDEXLIB_PATCHMODIFIERTEST_H
#define __INDEXLIB_PATCHMODIFIERTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/partition/modifier/patch_modifier.h"

IE_NAMESPACE_BEGIN(partition);

class PatchModifierTest : public INDEXLIB_TESTBASE
{
public:
    PatchModifierTest();
    ~PatchModifierTest();

    DECLARE_CLASS_NAME(PatchModifierTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestRemoveDocument();
    void TestRemoveDocumentFail();
    void TestUpdateDocument();
    void TestUpdateDocumentFailed();
    void TestUpdateDocumentWithoutSchema();
    void TestGeneratePatch();
    void TestSimpleProcessWithOnDiskPartitionData();
    void TestIsDirty();
    void TestGetTotalMemoryUseForSingleAttrUpdate();
    void TestGetTotalMemoryUseForVarNumAttrUpdate();

private:
    void InnerTestGeneratePatch(bool isOldFormatVersion);
    void InnerTestGetTotalMemoryUseForSingleAttrUpdate(bool patchCompress);
    void InnerTestGetTotalMemoryUseForVarNumAttrUpdate(bool patchCompress);
private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
    config::IndexPartitionOptions mOptions;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestRemoveDocument);
INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestUpdateDocument);
INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestGeneratePatch);
INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestSimpleProcessWithOnDiskPartitionData);
INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestUpdateDocumentFailed);
INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestUpdateDocumentWithoutSchema);
INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestRemoveDocumentFail);
INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestIsDirty);
INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestGetTotalMemoryUseForSingleAttrUpdate);
INDEXLIB_UNIT_TEST_CASE(PatchModifierTest, TestGetTotalMemoryUseForVarNumAttrUpdate);

IE_NAMESPACE_END(partition);

#endif //__INDEXLIB_PATCHMODIFIERTEST_H

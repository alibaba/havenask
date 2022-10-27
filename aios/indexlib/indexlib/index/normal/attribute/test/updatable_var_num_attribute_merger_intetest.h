#ifndef __INDEXLIB_UPDATABLEVARNUMATTRIBUTEMERGERINTETEST_H
#define __INDEXLIB_UPDATABLEVARNUMATTRIBUTEMERGERINTETEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

IE_NAMESPACE_BEGIN(index);

typedef std::tr1::tuple<bool, bool> CompressTypeParam;

class UpdatableVarNumAttributeMergerInteTest : 
    public INDEXLIB_TESTBASE_WITH_PARAM<CompressTypeParam>
{
public:
    UpdatableVarNumAttributeMergerInteTest();
    ~UpdatableVarNumAttributeMergerInteTest();

    DECLARE_CLASS_NAME(UpdatableVarNumAttributeMergerInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestMergeSegmentsWithPatch();
    void TestMergePatches();
    void TestMergeWithCompressOffset();

private:
    void CheckResult(const config::AttributeConfigPtr& attrConfig,
                     const std::string& expectResults);

    void CheckPatchFiles(const std::string& attrName, 
                         segmentid_t segId, const std::string& expectPatchFiles);

    void CheckOffsetLength(const std::string& attrName, 
                           segmentid_t segId, size_t expectOffsetLength);

    std::string MakeFieldTypeString(bool isCompress, bool isUniq);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_INSTANTIATE_TEST_CASE_ONE_P(
        UpdatableVarNumAttributeMergerInteTestCombineCompress, 
        UpdatableVarNumAttributeMergerInteTest, 
        testing::Combine(
                testing::Values(true, false),
                testing::Values(true, false)));

INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(UpdatableVarNumAttributeMergerInteTestCombineCompress, TestMergePatches);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(UpdatableVarNumAttributeMergerInteTestCombineCompress, TestMergeSegmentsWithPatch);
INDEXLIB_UNIT_TEST_CASE_WITH_PARAM(UpdatableVarNumAttributeMergerInteTestCombineCompress, TestMergeWithCompressOffset);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_UPDATABLEVARNUMATTRIBUTEMERGERINTETEST_H

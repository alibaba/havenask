#ifndef __INDEXLIB_UPDATABLEVARNUMATTRIBUTEMERGERTEST_H
#define __INDEXLIB_UPDATABLEVARNUMATTRIBUTEMERGERTEST_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_merger.h"

IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);

class UpdatableVarNumAttributeMergerTest : public INDEXLIB_TESTBASE
{
public:
    UpdatableVarNumAttributeMergerTest();
    ~UpdatableVarNumAttributeMergerTest();
public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestSimpleProcess();
    void TestMergeWithoutPatch();
    void TestMergeWithPatch();
    void TestMergeWithEmptyHeap();
    void TestCreatePatchReaderWithNoPatches();
    void TestMergePatchesLegacy();

private:
    template<typename T>
    AttributeMergerPtr CreateAttributeMerger(
            const std::string &dataAndPatchString);
    template<typename T>
    void CheckDataAndOffset(const AttributeMergerPtr &mergerPtr,
                            const std::string &expectDataAndOffsetString);
    template<typename T>
    void CheckMerge(const std::string &dataAndPatchString,
                    const std::string &segMergeInfosString,
                    const std::string &expectDataAndOffsetString);
    void CheckMergePatches(const std::string &segIdsInCurVersionString,
                           const std::string &segIdsInMergeString,
                           const std::string &patchesForSegIdString,
                           const std::string &expectMergePatchesString);

private:
    std::string mTestDataPath;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeMergerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeMergerTest, TestMergeWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeMergerTest, TestMergeWithPatch);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeMergerTest, TestMergeWithEmptyHeap);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeMergerTest, TestCreatePatchReaderWithNoPatches);
INDEXLIB_UNIT_TEST_CASE(UpdatableVarNumAttributeMergerTest, TestMergePatchesLegacy);

IE_NAMESPACE_END(index);
#endif //__INDEXLIB_UPDATABLEVARNUMATTRIBUTEMERGERTEST_H

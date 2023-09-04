#ifndef __INDEXLIB_UPDATABLEUNIQENCODEDVARNUMATTRIBUTEMERGERTEST_H
#define __INDEXLIB_UPDATABLEUNIQENCODEDVARNUMATTRIBUTEMERGERTEST_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/attribute/accessor/uniq_encoded_var_num_attribute_merger.h"
#include "indexlib/indexlib.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace indexlib::config;
using namespace indexlib::common;

namespace indexlib { namespace index {

class UpdatableUniqEncodedVarNumAttributeMergerTest : public INDEXLIB_TESTBASE
{
public:
    UpdatableUniqEncodedVarNumAttributeMergerTest();
    ~UpdatableUniqEncodedVarNumAttributeMergerTest();

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestMergeWithoutPatch();
    void TestUniqEncodeMergeWithoutPatch();
    void TestMergeWithPatch();
    void TestUniqEncodeMergeWithPatch();
    void TestMergeWithEmptyHeap();
    void TestFillOffsetToFirstOldDocIdMapVec();

private:
    template <typename T>
    AttributeMergerPtr CreateAttributeMerger(const std::string& dataAndPatchString);

    template <typename T>
    void CheckDataAndOffset(const AttributeMergerPtr& mergerPtr, const std::string& expectDataAndOffsetString);

    template <typename T>
    void CheckMerge(const std::string& dataAndPatchString, const std::string& segMergeInfosString,
                    const std::string& expectDataAndOffsetString);

private:
    std::string mTestDataPath;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(UpdatableUniqEncodedVarNumAttributeMergerTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(UpdatableUniqEncodedVarNumAttributeMergerTest, TestMergeWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(UpdatableUniqEncodedVarNumAttributeMergerTest, TestUniqEncodeMergeWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(UpdatableUniqEncodedVarNumAttributeMergerTest, TestMergeWithPatch);
INDEXLIB_UNIT_TEST_CASE(UpdatableUniqEncodedVarNumAttributeMergerTest, TestUniqEncodeMergeWithPatch);
INDEXLIB_UNIT_TEST_CASE(UpdatableUniqEncodedVarNumAttributeMergerTest, TestMergeWithEmptyHeap);
INDEXLIB_UNIT_TEST_CASE(UpdatableUniqEncodedVarNumAttributeMergerTest, TestFillOffsetToFirstOldDocIdMapVec);
}}     // namespace indexlib::index
#endif //__INDEXLIB_UPDATABLEUNIQENCODEDVARNUMATTRIBUTEMERGERTEST_H

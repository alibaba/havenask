#pragma once

#include "indexlib/common_define.h"
#include "indexlib/test/single_field_partition_data_provider.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class UpdatableAttributeMergerInteTest : public INDEXLIB_TESTBASE
{
public:
    UpdatableAttributeMergerInteTest();
    ~UpdatableAttributeMergerInteTest();

    DECLARE_CLASS_NAME(UpdatableAttributeMergerInteTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcessLongCaseTest();

private:
    void InnerTestAttributeMerge(uint32_t docCount, uint32_t docCountPerSegment, uint32_t updateRound, bool multiValue);

    void CheckResult(test::SingleFieldPartitionDataProvider& provider, const std::vector<std::string>& expectValues,
                     const std::string& mergeFootPrint);

    std::string PrepareFullDocs(uint32_t docCount, uint32_t docCountPerSegment, std::vector<std::string>& expectValues);

    std::string PrepareUpdateDocs(std::vector<std::string>& expectValues, uint32_t docCountPerSegment,
                                  uint32_t updateRoundIdx);

    std::string MakeRandomMergeStrategy(size_t updateRound, const index_base::Version& onDiskVersion,
                                        std::string& mergeFootPrint);

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(UpdatableAttributeMergerInteTest, TestSimpleProcessLongCaseTest);
}} // namespace indexlib::index

#include "indexlib/common_define.h"
#include "indexlib/config/schema_configurator.h"
#include "indexlib/index/calculator/partition_size_calculator.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

using namespace indexlib::config;

namespace indexlib { namespace index {

class PartitionSizeCalculatorTest : public INDEXLIB_TESTBASE
{
    DECLARE_CLASS_NAME(PartitionSizeCalculatorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

public:
    void TestCalculateVersionLockSizeWithoutPatch();
    void TestCalculateVersionLockSizeWithoutPatchWithSubSegment();
    void TestCalculatePkSize();

private:
    void MakeFile(const file_system::DirectoryPtr& dir, const std::string& fileName, size_t bytes);

    void PrepareSegment(segmentid_t segId);
    void PrepareSegmentData(segmentid_t segId, file_system::DirectoryPtr segDir, const std::string& attrName,
                            bool isSub);
    void PreparePkSegment(segmentid_t segId, const config::IndexPartitionSchemaPtr& schema, size_t docCount = 1,
                          size_t subDocCount = 1);
    void PreparePkSegmentData(const file_system::DirectoryPtr& segDir, const config::IndexPartitionSchemaPtr& schema,
                              size_t docCount);

private:
    IndexPartitionSchemaPtr mSchema;
    file_system::DirectoryPtr mRootDir;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, PartitionSizeCalculatorTest);

INDEXLIB_UNIT_TEST_CASE(PartitionSizeCalculatorTest, TestCalculateVersionLockSizeWithoutPatch);
INDEXLIB_UNIT_TEST_CASE(PartitionSizeCalculatorTest, TestCalculateVersionLockSizeWithoutPatchWithSubSegment);
INDEXLIB_UNIT_TEST_CASE(PartitionSizeCalculatorTest, TestCalculatePkSize);
}} // namespace indexlib::index

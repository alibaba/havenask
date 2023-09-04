#ifndef __INDEXLIB_PATCHLOADERTEST_H
#define __INDEXLIB_PATCHLOADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/partition/patch_loader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(config, IndexPartitionSchema);
DECLARE_REFERENCE_CLASS(file_system, Directory);

namespace indexlib { namespace partition {

class PatchLoaderTest : public INDEXLIB_TESTBASE
{
public:
    PatchLoaderTest();
    ~PatchLoaderTest();

    DECLARE_CLASS_NAME(PatchLoaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCalculatePatchFileSize();
    void TestCalculatePatchFileSizeWithCompress();
    void TestCalculatePackAttributeUpdateExpandSize();
    void TestInitWithLoadPatchThreadNum();
    void TestMultiThreadLoadPatch();
    void TestPatchWorkItemProcessMultipleTimes();

private:
    void PrepareSegment(segmentid_t segId, bool isCompressed = false);
    void PrepareSegmentData(segmentid_t segId, file_system::DirectoryPtr segDir, const std::string& attrName,
                            bool isSub, bool isCompressed = false);
    void MakeFile(const file_system::DirectoryPtr& dir, const std::string& fileName, size_t bytes,
                  bool isCompressed = false);
    size_t CalculatePatchFileSize(const index_base::PartitionDataPtr& partitionData,
                                  const config::AttributeConfigPtr& attrConfig);

private:
    config::IndexPartitionSchemaPtr mSchema;
    file_system::DirectoryPtr mRootDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(PatchLoaderTest, TestCalculatePatchFileSize);
INDEXLIB_UNIT_TEST_CASE(PatchLoaderTest, TestCalculatePatchFileSizeWithCompress);
INDEXLIB_UNIT_TEST_CASE(PatchLoaderTest, TestCalculatePackAttributeUpdateExpandSize);
INDEXLIB_UNIT_TEST_CASE(PatchLoaderTest, TestInitWithLoadPatchThreadNum);
INDEXLIB_UNIT_TEST_CASE(PatchLoaderTest, TestMultiThreadLoadPatch);
INDEXLIB_UNIT_TEST_CASE(PatchLoaderTest, TestPatchWorkItemProcessMultipleTimes);
}} // namespace indexlib::partition

#endif //__INDEXLIB_PATCHLOADERTEST_H

#ifndef __INDEXLIB_SEGMENTFILEMETATEST_H
#define __INDEXLIB_SEGMENTFILEMETATEST_H

#include "indexlib/common_define.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index_base/index_meta/segment_file_meta.h"
#include "indexlib/config/index_partition_schema.h"

IE_NAMESPACE_BEGIN(index_base);

class SegmentFileMetaTest : public INDEXLIB_TESTBASE
{
public:
    SegmentFileMetaTest();
    ~SegmentFileMetaTest();

    DECLARE_CLASS_NAME(SegmentFileMetaTest);
public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();
    void TestPackageFile();
    void TestSubSegment();
private:
    void PrepareIndex(bool hasSub, bool hasPackage);

private:
    std::string mRootDir;
    config::IndexPartitionSchemaPtr mSchema;
private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SegmentFileMetaTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(SegmentFileMetaTest, TestPackageFile);
INDEXLIB_UNIT_TEST_CASE(SegmentFileMetaTest, TestSubSegment);

IE_NAMESPACE_END(index_base);

#endif //__INDEXLIB_SEGMENTFILEMETATEST_H

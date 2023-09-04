#ifndef __INDEXLIB_SEGMENTDIRECTORYTEST_H
#define __INDEXLIB_SEGMENTDIRECTORYTEST_H

#include "indexlib/common_define.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/index_base/segment/segment_directory.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

DECLARE_REFERENCE_CLASS(index_base, Version);

namespace indexlib { namespace index_base {

class SegmentDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    SegmentDirectoryTest();
    ~SegmentDirectoryTest();

    DECLARE_CLASS_NAME(SegmentDirectoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestGetFsDirectoryForSub();
    void TestInitSegmentDataForSub();
    void TestInitIndexFormatVersion();
    void TestClone();
    void TestRollbackToCurrentVersion();

private:
    enum ExistStatus { FULL_EXISTS, NONE_EXISTS, SOME_EXISTS };
    ExistStatus CheckVersionExists(const index_base::Version& version);

private:
    std::string mRootDir;
    file_system::DirectoryPtr mDirectory;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestGetFsDirectoryForSub);
INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestInitSegmentDataForSub);
INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestInitIndexFormatVersion);
INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestClone);
INDEXLIB_UNIT_TEST_CASE(SegmentDirectoryTest, TestRollbackToCurrentVersion);
}} // namespace indexlib::index_base

#endif //__INDEXLIB_SEGMENTDIRECTORYTEST_H

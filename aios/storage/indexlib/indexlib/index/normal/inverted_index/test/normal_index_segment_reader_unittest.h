#ifndef __INDEXLIB_NORMALINDEXSEGMENTREADERTEST_H
#define __INDEXLIB_NORMALINDEXSEGMENTREADERTEST_H

#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/accessor/normal_index_segment_reader.h"
#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"

namespace indexlib { namespace index {

class NormalIndexSegmentReaderTest : public INDEXLIB_TESTBASE
{
public:
    NormalIndexSegmentReaderTest();
    ~NormalIndexSegmentReaderTest();

    DECLARE_CLASS_NAME(NormalIndexSegmentReaderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestOpenWithoutDictionary();
    void TestOpenWithoutPosting();
    void TestOpenWithoutDictionaryAndPosting();
    void TestOpenWithoutIndexNameDir();
    void TestOpen();
    void TestGetSegmentPosting();

private:
    std::string mRootDir;
    file_system::IFileSystemPtr mFileSystem;
    file_system::DirectoryPtr mLocalDir;
    file_system::DirectoryPtr mInMemDir;

private:
    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(NormalIndexSegmentReaderTest, TestOpen);
INDEXLIB_UNIT_TEST_CASE(NormalIndexSegmentReaderTest, TestOpenWithoutDictionary);
INDEXLIB_UNIT_TEST_CASE(NormalIndexSegmentReaderTest, TestOpenWithoutDictionaryAndPosting);
INDEXLIB_UNIT_TEST_CASE(NormalIndexSegmentReaderTest, TestOpenWithoutIndexNameDir);
INDEXLIB_UNIT_TEST_CASE(NormalIndexSegmentReaderTest, TestGetSegmentPosting);
}} // namespace indexlib::index

#endif //__INDEXLIB_NORMALINDEXSEGMENTREADERTEST_H

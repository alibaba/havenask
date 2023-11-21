#include "indexlib/file_system/file/BufferedFileNode.h"

#include "gtest/gtest.h"
#include <iosfwd>
#include <memory>
#include <stdint.h>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/file_system/test/TestFileCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/byte_slice_list/ByteSliceList.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class FileIOException;
class UnSupportedException;
}} // namespace indexlib::util

using namespace std;

using namespace indexlib::util;

namespace indexlib { namespace file_system {

class BufferedFileNodeTest : public INDEXLIB_TESTBASE
{
public:
    BufferedFileNodeTest();
    ~BufferedFileNodeTest();

    DECLARE_CLASS_NAME(BufferedFileNodeTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestNormalCase();
    void TestFileNotExist();
    void TestBufferedFileReaderNotSupport();

private:
    std::string _rootDir;
    std::shared_ptr<IFileSystem> _fileSystem;
    std::shared_ptr<Directory> _directory;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, BufferedFileNodeTest);

INDEXLIB_UNIT_TEST_CASE(BufferedFileNodeTest, TestNormalCase);
INDEXLIB_UNIT_TEST_CASE(BufferedFileNodeTest, TestFileNotExist);
INDEXLIB_UNIT_TEST_CASE(BufferedFileNodeTest, TestBufferedFileReaderNotSupport);
//////////////////////////////////////////////////////////////////////

BufferedFileNodeTest::BufferedFileNodeTest() {}

BufferedFileNodeTest::~BufferedFileNodeTest() {}

void BufferedFileNodeTest::CaseSetUp()
{
    _rootDir = PathUtil::NormalizePath(GET_TEMPLATE_DATA_PATH());
    _fileSystem = FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow();
    _directory = Directory::Get(_fileSystem);
}

void BufferedFileNodeTest::CaseTearDown() {}

void BufferedFileNodeTest::TestNormalCase()
{
    string filePath = _rootDir + "buffered_file";
    string fileContext = "buffered file test data";
    FileSystemTestUtil::CreateDiskFile(filePath, fileContext);
    BufferedFileNode fileNode;
    ASSERT_EQ(FSEC_OK, fileNode.Open("LOGICAL_PATH", filePath, FSOT_BUFFERED, -1));

    ASSERT_EQ(FSFT_BUFFERED, fileNode.GetType());
    ASSERT_EQ(fileContext.size(), fileNode.GetLength());
    ASSERT_EQ(filePath, fileNode.GetPhysicalPath());
    ASSERT_FALSE(fileNode.GetBaseAddress());
    uint8_t buffer[1024];
    ASSERT_EQ(fileContext.size(), fileNode.Read(buffer, fileContext.size(), 0, ReadOption()).GetOrThrow());

    ASSERT_EQ(nullptr, fileNode.ReadToByteSliceList(fileContext.size(), 0, ReadOption()));
    ASSERT_EQ(FSEC_NOTSUP, fileNode.Write(buffer, 1).Code());
    ASSERT_EQ(FSEC_OK, fileNode.Close());
    ASSERT_EQ(FSEC_OK, fileNode.Close());
}

void BufferedFileNodeTest::TestFileNotExist()
{
    BufferedFileNode fileNode;
    string path = _rootDir + "not_exist_file";
    ASSERT_EQ(FSEC_NOENT, fileNode.Open("LOGICAL_PATH", path, FSOT_BUFFERED, -1));
}

void BufferedFileNodeTest::TestBufferedFileReaderNotSupport()
{
    std::shared_ptr<IFileSystem> fileSystem = _fileSystem;

    string fileContext = "hello";
    std::shared_ptr<FileReader> fileReader =
        TestFileCreator::CreateReader(fileSystem, FSST_DISK, FSOT_BUFFERED, fileContext);

    uint8_t buffer[1024];
    ASSERT_EQ(fileContext.size() - 1, fileReader->Read(buffer, fileContext.size(), 1).GetOrThrow());
    ASSERT_EQ(nullptr, fileReader->ReadToByteSliceList(fileContext.size(), 0, ReadOption()));
    ASSERT_FALSE(fileReader->GetBaseAddress());
}
}} // namespace indexlib::file_system

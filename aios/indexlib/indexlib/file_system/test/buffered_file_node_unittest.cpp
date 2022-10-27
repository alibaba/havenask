#include "indexlib/misc/exception.h"
#include "indexlib/file_system/file_system_define.h"
#include "indexlib/file_system/test/test_file_creator.h"
#include "indexlib/file_system/test/file_system_test_util.h"
#include "indexlib/file_system/test/buffered_file_node_unittest.h"

using namespace std;

IE_NAMESPACE_USE(misc);

IE_NAMESPACE_BEGIN(file_system);
IE_LOG_SETUP(file_system, BufferedFileNodeTest);

BufferedFileNodeTest::BufferedFileNodeTest()
{
}

BufferedFileNodeTest::~BufferedFileNodeTest()
{
}

void BufferedFileNodeTest::CaseSetUp()
{
    mRootDir = GET_TEST_DATA_PATH();
}

void BufferedFileNodeTest::CaseTearDown()
{
}

void BufferedFileNodeTest::TestNormalCase()
{
    string filePath = mRootDir + "buffered_file";
    string fileContext = "buffered file test data";
    FileSystemTestUtil::CreateDiskFile(filePath, fileContext);
    BufferedFileNode fileNode;
    fileNode.Open(filePath, FSOT_BUFFERED);

    ASSERT_EQ(FSFT_BUFFERED, fileNode.GetType());
    ASSERT_EQ(fileContext.size(), fileNode.GetLength());
    ASSERT_EQ(filePath, fileNode.GetPath());
    ASSERT_FALSE(fileNode.GetBaseAddress());
    uint8_t buffer[1024];
    ASSERT_EQ(fileContext.size(), fileNode.Read(buffer, fileContext.size(), 0));

    ASSERT_THROW(fileNode.Read(fileContext.size(), 0), UnSupportedException);
    ASSERT_THROW(fileNode.Write(buffer, 1), UnSupportedException);
    ASSERT_NO_THROW(fileNode.Close());
    ASSERT_NO_THROW(fileNode.Close());
}

void BufferedFileNodeTest::TestFileNotExist()
{
    BufferedFileNode fileNode;
    string path = mRootDir + "not_exist_file";
    ASSERT_THROW(fileNode.Open(mRootDir, FSOT_BUFFERED), FileIOException);
}

void BufferedFileNodeTest::TestBufferedFileReaderNotSupport()
{
    IndexlibFileSystemPtr fileSystem = GET_FILE_SYSTEM();

    string fileContext = "hello";
    FileReaderPtr fileReader = TestFileCreator::CreateReader(
            fileSystem, FSST_LOCAL, FSOT_BUFFERED, fileContext);

    uint8_t buffer[1024];
    ASSERT_EQ(fileContext.size() - 1, 
              fileReader->Read(buffer, fileContext.size(), 1));
    ASSERT_THROW(fileReader->Read(fileContext.size(), 0),
                 UnSupportedException);
    ASSERT_FALSE(fileReader->GetBaseAddress());
}

IE_NAMESPACE_END(file_system);


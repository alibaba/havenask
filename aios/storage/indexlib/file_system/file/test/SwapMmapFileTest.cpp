#include "gtest/gtest.h"
#include <cstddef>
#include <memory>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/WriterOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/file_system/file/SwapMmapFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class SwapMmapFileTest : public INDEXLIB_TESTBASE
{
public:
    SwapMmapFileTest();
    ~SwapMmapFileTest();

    DECLARE_CLASS_NAME(SwapMmapFileTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    void InnerTestSimpleProcess(bool autoStore);

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, SwapMmapFileTest);

INDEXLIB_UNIT_TEST_CASE(SwapMmapFileTest, TestSimpleProcess);
//////////////////////////////////////////////////////////////////////

SwapMmapFileTest::SwapMmapFileTest() {}

SwapMmapFileTest::~SwapMmapFileTest() {}

void SwapMmapFileTest::CaseSetUp() {}

void SwapMmapFileTest::CaseTearDown() {}

void SwapMmapFileTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    InnerTestSimpleProcess(true);
    InnerTestSimpleProcess(false);
}

void SwapMmapFileTest::InnerTestSimpleProcess(bool autoStore)
{
    tearDown();
    setUp();

    FileSystemOptions options;
    options.outputStorage = FSST_MEM;
    std::shared_ptr<Directory> segDir = Directory::Get(
        FileSystemCreator::CreateForWrite("SwapMmapFileTest", GET_TEMP_DATA_PATH(), options).GetOrThrow());

    // create file
    SwapMmapFileWriterPtr smFileWriter = std::dynamic_pointer_cast<SwapMmapFileWriter>(
        segDir->CreateFileWriter("swap_mmap_file", WriterOption::SwapMmap(1024)));
    ASSERT_TRUE(smFileWriter != nullptr);

    // write file
    string content = "abc";
    ASSERT_EQ(size_t(3), smFileWriter->Write(content.c_str(), 3).GetOrThrow());

    // read file
    std::shared_ptr<FileReader> smFileReader = segDir->CreateFileReader("swap_mmap_file", ReaderOption::SwapMmap());
    char* baseAddr = (char*)smFileReader->GetBaseAddress();
    ASSERT_EQ(content, string(baseAddr, 3));
    ASSERT_EQ((size_t)3, smFileReader->GetLength());

    // get & read file
    std::shared_ptr<FileReader> smFileReader1 = segDir->CreateFileReader("swap_mmap_file", ReaderOption::SwapMmap());
    ASSERT_TRUE(smFileReader1 != nullptr);
    baseAddr[0] = 'b';
    char* newBaseAddr = (char*)smFileReader1->GetBaseAddress();
    ASSERT_EQ(string("bbc"), string(newBaseAddr, 3));

    // sync & truncate file
    smFileWriter->TEST_Sync();

    string loadStr;
    string filePath = GET_TEMP_DATA_PATH() + "/" + segDir->GetLogicalPath() + "/swap_mmap_file";
    ASSERT_EQ(FSEC_OK, FslibWrapper::AtomicLoad(filePath, loadStr).Code());
    ASSERT_EQ(string("bbc"), loadStr.substr(0, 3));

    if (autoStore) {
        ASSERT_EQ(FSEC_OK, smFileWriter->Close());
    }

    smFileWriter.reset();
    smFileReader.reset();
    smFileReader1.reset();

    segDir->GetFileSystem()->CleanCache();
    ASSERT_EQ(autoStore, FslibWrapper::IsExist(filePath).GetOrThrow());
}
}} // namespace indexlib::file_system

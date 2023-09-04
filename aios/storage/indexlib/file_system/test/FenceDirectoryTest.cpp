#include "indexlib/file_system/FenceDirectory.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/TempDirectory.h"
#include "indexlib/file_system/file/TempFileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/test/FileSystemTestUtil.h"
#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class FenceDirectoryTest : public INDEXLIB_TESTBASE
{
public:
    FenceDirectoryTest();
    ~FenceDirectoryTest();

    DECLARE_CLASS_NAME(FenceDirectoryTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestUsage();
    void TestRemoveAndCreate();
    void TestNoRemovePreFile();

private:
    std::shared_ptr<Directory> _rootDirectory;
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(FenceDirectoryTest, TestUsage);
INDEXLIB_UNIT_TEST_CASE(FenceDirectoryTest, TestRemoveAndCreate);
INDEXLIB_UNIT_TEST_CASE(FenceDirectoryTest, TestNoRemovePreFile);

AUTIL_LOG_SETUP(indexlib.file_system, FenceDirectoryTest);

FenceDirectoryTest::FenceDirectoryTest() {}

FenceDirectoryTest::~FenceDirectoryTest() {}

void FenceDirectoryTest::CaseSetUp()
{
    auto fs = FileSystemCreator::Create("", GET_TEMP_DATA_PATH(), FileSystemOptions::Offline()).GetOrThrow();
    _rootDirectory = Directory::Get(fs);
}

void FenceDirectoryTest::CaseTearDown() {}

void FenceDirectoryTest::TestUsage()
{
    {
        auto dir = Directory::ConstructByFenceContext(_rootDirectory,
                                                      std::shared_ptr<FenceContext>(FenceContext::NoFence()), nullptr);
        ASSERT_FALSE(std::dynamic_pointer_cast<FenceDirectory>(dir->GetIDirectory()));
    }
    {
        std::shared_ptr<FenceContext> fenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "12"));
        auto dir = Directory::ConstructByFenceContext(_rootDirectory, fenceContext, nullptr);
        ASSERT_TRUE(std::dynamic_pointer_cast<FenceDirectory>(dir->GetIDirectory()));
        ASSERT_FALSE(dir->GetFenceContext()->hasPrepareHintFile);

        auto tempDir = dir->MakeDirectory("hhh");
        ASSERT_TRUE(std::dynamic_pointer_cast<TempDirectory>(tempDir->GetIDirectory()));
        std::shared_ptr<FileWriter> writer = tempDir->CreateFileWriter("data", WriterOption::Buffer());
        ASSERT_EQ(FSEC_OK, writer->WriteVUInt32(12));
        ASSERT_EQ(FSEC_OK, writer->Close());

        // temp directory not closed, you can't see it
        ASSERT_FALSE(dir->GetDirectory("hhh", false));
        FileList files;
        dir->ListDir("", files, false);
        ASSERT_EQ(FileList {}, files);

        // after closed, you will see it
        tempDir->Close();
        dir->ListDir("", files, true);
        sort(files.begin(), files.end());
        FileList expected = {"hhh/", "hhh/data"};
        ASSERT_EQ(expected, files);

        // you can't use tempDir when closed
        ASSERT_THROW(tempDir->MakeDirectory("noway"), util::FileIOException);
        ASSERT_THROW(tempDir->CreateFileWriter("noway"), util::FileIOException);

        // you can use @MakeDirectory or @GetDirectory to get existed directory
        tempDir = dir->MakeDirectory("hhh");
        ASSERT_TRUE(tempDir);
        ASSERT_TRUE(std::dynamic_pointer_cast<FenceDirectory>(tempDir->GetIDirectory()));

        // low epoch can't operate
        fenceContext.reset(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "11"));
        auto otherDir = Directory::ConstructByFenceContext(_rootDirectory, fenceContext, nullptr);
        ASSERT_TRUE(otherDir->GetDirectory("hhh", true));
        ASSERT_THROW(otherDir->RemoveDirectory("hhh"), util::FileIOException);

        CaseSetUp();
        fenceContext.reset(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "13"));

        otherDir = Directory::ConstructByFenceContext(_rootDirectory, fenceContext, nullptr);
        otherDir->RemoveDirectory("hhh");
        files.clear();
        otherDir->ListDir("", files);
        ASSERT_EQ(FileList {}, files);
    }
    {
        std::shared_ptr<FenceContext> fenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "14"));
        auto dir = Directory::ConstructByFenceContext(_rootDirectory, fenceContext, nullptr);
        ASSERT_TRUE(std::dynamic_pointer_cast<FenceDirectory>(dir->GetIDirectory()));

        auto tempWriter = dir->CreateFileWriter("data", WriterOption::Buffer());
        ASSERT_TRUE(std::dynamic_pointer_cast<TempFileWriter>(tempWriter));
        ASSERT_EQ(FSEC_OK, tempWriter->WriteVUInt32(12));
        ASSERT_FALSE(dir->IsExist("data"));

        ASSERT_EQ(FSEC_OK, tempWriter->Close());
        ASSERT_TRUE(dir->IsExist("data"));
    }
}

void FenceDirectoryTest::TestRemoveAndCreate()
{
    auto segmentDir = _rootDirectory->MakeDirectory("segment_816_level_0");
    auto writer = segmentDir->CreateFileWriter("data", WriterOption::Buffer());
    ASSERT_EQ(FSEC_OK, writer->WriteVUInt32(12));
    ASSERT_EQ(FSEC_OK, writer->Close());

    std::shared_ptr<FenceContext> fenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "12"));
    auto dir = Directory::ConstructByFenceContext(_rootDirectory, fenceContext, nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<FenceDirectory>(dir->GetIDirectory()));
    ASSERT_FALSE(dir->GetFenceContext()->hasPrepareHintFile);

    if (dir->IsExist("segment_816_level_0")) {
        dir->RemoveDirectory("segment_816_level_0");
        ASSERT_TRUE(dir->GetFenceContext()->hasPrepareHintFile);
    }
    segmentDir = dir->MakeDirectory("segment_816_level_0");
    ASSERT_TRUE(std::dynamic_pointer_cast<TempDirectory>(segmentDir->GetIDirectory()));
    segmentDir->Close();
}

void FenceDirectoryTest::TestNoRemovePreFile()
{
    auto segmentDir = _rootDirectory->MakeDirectory("segment_816_level_0");
    auto writer = segmentDir->CreateFileWriter("data", WriterOption::Buffer());
    ASSERT_EQ(FSEC_OK, writer->WriteVUInt32(12));
    ASSERT_EQ(FSEC_OK, writer->Close());

    std::shared_ptr<FenceContext> fenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "12"));
    auto dir = Directory::ConstructByFenceContext(_rootDirectory, fenceContext, nullptr);
    ASSERT_TRUE(std::dynamic_pointer_cast<FenceDirectory>(dir->GetIDirectory()));

    if (dir->IsExist("segment_816_level_0")) {
        dir->RemoveDirectory("segment_816_level_0");
    }
    segmentDir = dir->MakeDirectory("segment_816_level_0");
    writer = segmentDir->CreateFileWriter("data", WriterOption::Buffer());
    ASSERT_EQ(FSEC_OK, writer->WriteVUInt32(12));
    ASSERT_EQ(FSEC_OK, writer->Close());
    ASSERT_THROW(dir->Close(), util::FileIOException);
    segmentDir->Close();
    dir->Close();
    ASSERT_FALSE(dir->IsExist("pre" + string(TEMP_FILE_SUFFIX)));
}

}} // namespace indexlib::file_system

#include "indexlib/framework/index_task/IndexOperation.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/IFileSystem.h"
#include "indexlib/file_system/ReaderOption.h"
#include "indexlib/file_system/file/FileReader.h"
#include "indexlib/framework/index_task/IndexTaskContextCreator.h"
#include "indexlib/util/memory_control/MemoryQuotaController.h"
#include "indexlib/util/memory_control/PartitionMemoryQuotaController.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class IndexOperationTest : public TESTBASE
{
public:
    void setUp() override
    {
        indexlib::file_system::FileSystemOptions fileSystemOptions;
        fileSystemOptions.needFlush = true;
        indexlib::util::MemoryQuotaControllerPtr mqc(
            new indexlib::util::MemoryQuotaController(std::numeric_limits<int64_t>::max()));
        indexlib::util::PartitionMemoryQuotaControllerPtr quotaController(
            new indexlib::util::PartitionMemoryQuotaController(mqc));
        fileSystemOptions.memoryQuotaController = quotaController;
        _fileSystem =
            indexlib::file_system::FileSystemCreator::Create(/*name=*/"uninitialized-name",
                                                             /*rootPath=*/GET_TEMP_DATA_PATH(), fileSystemOptions,
                                                             /*metricsProvider=*/nullptr,
                                                             /*isOverride=*/false)
                .GetOrThrow();
        _indexRoot = indexlib::file_system::IDirectory::Get(_fileSystem);
        auto [status, dir] =
            _indexRoot->MakeDirectory("__FENCE__test", indexlib::file_system::DirectoryOption()).StatusWith();
        _fenceDir = dir;
    }

private:
    std::shared_ptr<indexlib::file_system::IDirectory> _indexRoot;
    std::shared_ptr<indexlib::file_system::IDirectory> _fenceDir;
    std::shared_ptr<indexlib::file_system::IFileSystem> _fileSystem;
};

class FakeIndexOperation : public IndexOperation
{
    FakeIndexOperation(IndexOperationId opId, bool useOpFenceDir) : IndexOperation(opId, useOpFenceDir) {}
    Status Execute(const IndexTaskContext& context) override { return Status::OK(); }
};

TEST_F(IndexOperationTest, testPublish)
{
    FakeIndexOperation op(/*opId=*/0, /*useOpFenceDir=*/false);
    IndexTaskContext ctx;
    ctx.TEST_SetIndexRoot(indexlib::file_system::IDirectory::ToLegacyDirectory(_indexRoot));
    ctx.TEST_SetFenceRoot(indexlib::file_system::IDirectory::ToLegacyDirectory(_fenceDir));
    std::string fileName = "file1";
    std::string content = "content1";
    ASSERT_TRUE(op.Publish(ctx, "segment_0/", fileName, content).IsOK());
    auto [status, isExist] = _indexRoot->IsExist("segment_0/file1").StatusWith();
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(isExist);
    auto [status1, reader] =
        _indexRoot
            ->CreateFileReader("segment_0/file1",
                               indexlib::file_system::ReaderOption(indexlib::file_system::FSOpenType::FSOT_MEM))
            .StatusWith();
    ASSERT_TRUE(status1.IsOK());
    ASSERT_EQ(reader->GetLength(), content.size());
}

} // namespace indexlibv2::framework

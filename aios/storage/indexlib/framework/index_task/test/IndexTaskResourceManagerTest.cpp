#include "indexlib/framework/index_task/IndexTaskResourceManager.h"

#include <string>

#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/IDirectory.h"
#include "indexlib/file_system/LogicalFileSystem.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/framework/index_task/IIndexTaskResourceCreator.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"
#include "indexlib/framework/index_task/test/FakeResource.h"
#include "indexlib/util/PathUtil.h"
#include "unittest/unittest.h"

namespace indexlibv2::framework {

class IndexTaskResourceManagerTest : public TESTBASE
{
public:
    IndexTaskResourceManagerTest() {}
    ~IndexTaskResourceManagerTest() {}

    void setUp() override
    {
        _testRoot = GET_TEMP_DATA_PATH();
        _fs = indexlib::file_system::FileSystemCreator::Create("test", _testRoot).GetOrThrow();
        _resourceDir = indexlib::file_system::Directory::Get(_fs);
    }
    void tearDown() override {}

private:
    std::string _testRoot;
    indexlib::file_system::IFileSystemPtr _fs;
    indexlib::file_system::DirectoryPtr _resourceDir;
};

TEST_F(IndexTaskResourceManagerTest, testSimpleStoreLoad)
{
    std::vector<int> data = {1, 2, 3};
    {
        IndexTaskResourceManager manager;
        auto resourceCreator = std::make_unique<FakeResourceCreator>();
        ASSERT_TRUE(manager.Init(_resourceDir, "work", std::move(resourceCreator)).IsOK());
        auto [status, resource] = manager.LoadResource(/*name*/ "hello", /*type*/ "vector");
        ASSERT_FALSE(status.IsOK());
        std::shared_ptr<FakeVectorResource> vectorResource;
        status = manager.CreateResource(/*name*/ "hello", /*type*/ "vector", vectorResource);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(vectorResource);
        vectorResource->SetData(data);
        status = manager.CommitResource(/*name*/ "hello");
        ASSERT_TRUE(status.IsOK());
    }
    {
        IndexTaskResourceManager manager;
        auto resourceCreator = std::make_unique<FakeResourceCreator>();
        ASSERT_TRUE(manager.Init(_resourceDir, "work2", std::move(resourceCreator)).IsOK());
        std::shared_ptr<FakeVectorResource> vectorResource;
        auto status = manager.CreateResource(/*name*/ "hello", /*type*/ "vector", vectorResource);
        ASSERT_TRUE(status.IsOK());
        ASSERT_TRUE(vectorResource);
        vectorResource->SetData(data);
        status = manager.CommitResource(/*name*/ "hello");
        ASSERT_TRUE(status.IsOK());
    }
    {
        IndexTaskResourceManager manager;
        auto resourceCreator = std::make_unique<FakeResourceCreator>();
        ASSERT_TRUE(manager.Init(_resourceDir, "work2", std::move(resourceCreator)).IsOK());
        auto [status, resource] = manager.LoadResource(/*name*/ "hello", /*type*/ "vector");
        ASSERT_TRUE(status.IsOK());
        auto vectorResource = dynamic_cast<FakeVectorResource*>(resource.get());
        ASSERT_TRUE(vectorResource);
        ASSERT_EQ(data, vectorResource->GetData());

        auto [status1, resource1] = manager.LoadResource(/*name*/ "hello", /*type*/ "vector");
        ASSERT_TRUE(status.IsOK());
        ASSERT_EQ(resource.get(), resource1.get());
    }
}

TEST_F(IndexTaskResourceManagerTest, testLoadNonExistResource)
{
    IndexTaskResourceManager manager;
    auto resourceCreator = std::make_unique<FakeResourceCreator>();
    ASSERT_TRUE(manager.Init(_resourceDir, "work", std::move(resourceCreator)).IsOK());

    auto [status, resource] = manager.LoadResource(/*name*/ "hello", /*type*/ "vector");
    ASSERT_TRUE(status.IsNotFound());
    ASSERT_FALSE(resource);
}

TEST_F(IndexTaskResourceManagerTest, testCreateWrongTypeResource)
{
    IndexTaskResourceManager manager;
    auto resourceCreator = std::make_unique<FakeResourceCreator>();
    ASSERT_TRUE(manager.Init(_resourceDir, "work", std::move(resourceCreator)).IsOK());
    std::shared_ptr<FakeVectorResource> vectorResource;
    auto status = manager.CreateResource(/*name*/ "hello", /*type*/ "queue", vectorResource);
    ASSERT_TRUE(status.IsCorruption());
}

TEST_F(IndexTaskResourceManagerTest, testLoadWrongTypeResource)
{
    IndexTaskResourceManager manager;
    auto resourceCreator = std::make_unique<FakeResourceCreator>();
    ASSERT_TRUE(manager.Init(_resourceDir, "work", std::move(resourceCreator)).IsOK());
    std::shared_ptr<FakeVectorResource> vectorResource;
    auto status = manager.CreateResource(/*name*/ "hello", /*type*/ "vector", vectorResource);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(vectorResource);

    auto [status1, resource1] = manager.LoadResource(/*name*/ "hello", /*type*/ "queue");
    ASSERT_TRUE(status1.IsCorruption());
}

TEST_F(IndexTaskResourceManagerTest, testLinkFileNonExist)
{
    {
        IndexTaskResourceManager manager;
        auto resourceCreator = std::make_unique<FakeResourceCreator>();
        ASSERT_TRUE(manager.Init(_resourceDir, "work", std::move(resourceCreator)).IsOK());
        std::shared_ptr<FakeVectorResource> vectorResource;
        auto status = manager.CreateResource(/*name*/ "hello", /*type*/ "vector", vectorResource);
        ASSERT_TRUE(status.IsOK());
        status = manager.CommitResource(/*name*/ "hello");
        ASSERT_TRUE(status.IsOK());

        auto linkFileName = IndexTaskResourceManager::GetLinkFileName(/*name*/ "hello");
        indexlib::file_system::RemoveOption removeOption;
        status = _resourceDir->GetIDirectory()->RemoveFile(linkFileName, removeOption).Status();
        ASSERT_TRUE(status.IsOK());
        manager._resources.clear();
    }
    {
        IndexTaskResourceManager manager;
        auto resourceCreator = std::make_unique<FakeResourceCreator>();
        ASSERT_TRUE(manager.Init(_resourceDir, "work", std::move(resourceCreator)).IsOK());
        auto [status1, resource1] = manager.LoadResource(/*name*/ "hello", /*type*/ "vector");
        ASSERT_TRUE(status1.IsNotFound());
    }
}

TEST_F(IndexTaskResourceManagerTest, testCommitOnExistResource)
{
    std::vector<int> data = {1, 2, 3};
    {
        IndexTaskResourceManager manager;
        auto resourceCreator = std::make_unique<FakeResourceCreator>();
        ASSERT_TRUE(manager.Init(_resourceDir, "work", std::move(resourceCreator)).IsOK());
        std::shared_ptr<FakeVectorResource> vectorResource;
        auto status = manager.CreateResource(/*name*/ "hello", /*type*/ "vector", vectorResource);
        status = manager.CommitResource(/*name*/ "hello");
        ASSERT_TRUE(status.IsOK());

        status = manager.CreateResource(/*name*/ "hello", /*type*/ "vector", vectorResource);
        ASSERT_TRUE(status.IsExist());
    }
    {
        IndexTaskResourceManager manager;
        auto resourceCreator = std::make_unique<FakeResourceCreator>();
        ASSERT_TRUE(manager.Init(_resourceDir, "work2", std::move(resourceCreator)).IsOK());
        std::shared_ptr<FakeVectorResource> vectorResource;
        auto status = manager.CreateResource(/*name*/ "hello", /*type*/ "vector", vectorResource);
        status = manager.CommitResource(/*name*/ "hello");
        ASSERT_TRUE(status.IsOK());
    }
}

TEST_F(IndexTaskResourceManagerTest, testCommitAfterFail)
{
    auto workDir = _resourceDir->MakeDirectory("work");
    auto helloDir = workDir->MakeDirectory(IndexTaskResourceManager::GetResourceDirName("hello"));
    helloDir->Store("data", "helloworld");

    IndexTaskResourceManager manager;
    auto resourceCreator = std::make_unique<FakeResourceCreator>();
    ASSERT_TRUE(manager.Init(_resourceDir, "work", std::move(resourceCreator)).IsOK());
    std::shared_ptr<FakeVectorResource> vectorResource;
    auto status = manager.CreateResource(/*name*/ "hello", /*type*/ "vector", vectorResource);
    status = manager.CommitResource(/*name*/ "hello");
    ASSERT_TRUE(status.IsOK());
}

TEST_F(IndexTaskResourceManagerTest, testReleaseResource)
{
    IndexTaskResourceManager manager;
    auto resourceCreator = std::make_unique<FakeResourceCreator>();
    ASSERT_TRUE(manager.Init(_resourceDir, "work", std::move(resourceCreator)).IsOK());
    std::shared_ptr<IndexTaskResource> resource;
    auto status = manager.CreateResource(/*name*/ "hello", /*type*/ "vector", resource);
    ASSERT_TRUE(status.IsOK());
    ASSERT_TRUE(resource);

    std::tie(status, resource) = manager.LoadResource(/*name*/ "hello", /*type*/ "vector");
    ASSERT_TRUE(status.IsOK());
    status = manager.ReleaseResource("hello");
    ASSERT_TRUE(status.IsOK());
    std::tie(status, resource) = manager.LoadResource(/*name*/ "hello", /*type*/ "vector");
    ASSERT_TRUE(status.IsNotFound());
}

} // namespace indexlibv2::framework

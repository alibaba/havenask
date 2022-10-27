#include "indexlib/index_base/index_meta/test/merge_task_resource_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/file_system/indexlib_file_system_creator.h"
#include "indexlib/file_system/directory_creator.h"

using namespace std;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(file_system);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, MergeTaskResourceTest);

MergeTaskResourceTest::MergeTaskResourceTest()
{
}

MergeTaskResourceTest::~MergeTaskResourceTest()
{
}

void MergeTaskResourceTest::CaseSetUp()
{
}

void MergeTaskResourceTest::CaseTearDown()
{
}

void MergeTaskResourceTest::TestSimpleProcess()
{
    string root = GET_TEST_DATA_PATH();
    IndexlibFileSystemPtr fs = IndexlibFileSystemCreator::Create(root);
    DirectoryPtr rootDir = DirectoryCreator::Get(fs, root, true);
    MergeTaskResource res(rootDir, 10);
    string value = "hello world";
    res.Store(value.data(), value.length());

    string filePath = MergeTaskResource::GetResourceFileName(10);
    ASSERT_TRUE(rootDir->IsExist(filePath));
    ASSERT_EQ(res.GetDataLen(), rootDir->GetFileLength(filePath));

    string jsonStr = ToJsonString(res);
    MergeTaskResource newRes;
    ASSERT_FALSE(newRes.IsValid());
    FromJsonString(newRes, jsonStr);
    newRes.SetRoot(rootDir);
    ASSERT_TRUE(newRes.IsValid());

    ASSERT_EQ(res.GetResourceId(), newRes.GetResourceId());
    ASSERT_EQ(res.GetDataLen(), newRes.GetDataLen());
    ASSERT_EQ(res.GetRootPath(), newRes.GetRootPath());

    ASSERT_EQ(0, memcmp(res.GetData(), newRes.GetData(), res.GetDataLen()));

    char buffer[128];
    ASSERT_EQ(res.GetDataLen(), newRes.Read(buffer, 128));
    ASSERT_EQ(value, string(buffer, newRes.GetDataLen()));
}

IE_NAMESPACE_END(index_base);


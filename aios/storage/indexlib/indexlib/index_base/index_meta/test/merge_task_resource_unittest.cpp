#include "indexlib/index_base/index_meta/test/merge_task_resource_unittest.h"

#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/test/directory_creator.h"

using namespace std;
using namespace indexlib::file_system;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, MergeTaskResourceTest);

MergeTaskResourceTest::MergeTaskResourceTest() {}

MergeTaskResourceTest::~MergeTaskResourceTest() {}

void MergeTaskResourceTest::CaseSetUp() {}

void MergeTaskResourceTest::CaseTearDown() {}

void MergeTaskResourceTest::TestSimpleProcess()
{
    DirectoryPtr rootDir = GET_PARTITION_DIRECTORY();
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
}} // namespace indexlib::index_base

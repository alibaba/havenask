#include <autil/StringUtil.h>
#include "indexlib/index_base/test/merge_task_resource_manager_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/util/env_util.h"

using namespace std;
using namespace autil;
IE_NAMESPACE_USE(storage);
IE_NAMESPACE_USE(util);

IE_NAMESPACE_BEGIN(index_base);
IE_LOG_SETUP(index_base, MergeTaskResourceManagerTest);

MergeTaskResourceManagerTest::MergeTaskResourceManagerTest()
{
}

MergeTaskResourceManagerTest::~MergeTaskResourceManagerTest()
{
}

void MergeTaskResourceManagerTest::CaseSetUp()
{
}

void MergeTaskResourceManagerTest::CaseTearDown()
{
}

void MergeTaskResourceManagerTest::TestSimpleProcess()
{
    EnvUtil::SetEnv("INDEXLIB_PACKAGE_MERGE_META", "true");
    InnerTest();
    EnvUtil::UnsetEnv("INDEXLIB_PACKAGE_MERGE_META");
    InnerTest();
}

void MergeTaskResourceManagerTest::InnerTest()
{
    string rootPath = FileSystemWrapper::JoinPath(
            GET_TEST_DATA_PATH(), "merge_resource");
    rootPath = FileSystemWrapper::NormalizeDir(rootPath);
    MergeTaskResourceManager mgr;
    mgr.Init(rootPath);
    for (resourceid_t i = 0; i < 20; i++)
    {
        string data = MakeDataForResource(i);
        resourceid_t id = mgr.DeclareResource(data.c_str(), data.size(),
                StringUtil::toString(i));
        ASSERT_EQ(i, id);
    }
    mgr.Commit();
    
    string jsonStr = ToJsonString(mgr.GetResourceVec());
    MergeTaskResourceManager::MergeTaskResourceVec resVec;
    FromJsonString(resVec, jsonStr);

    MergeTaskResourceManager newMgr;
    newMgr.Init(resVec);
    ASSERT_EQ(rootPath, FileSystemWrapper::NormalizeDir(newMgr.GetRootPath()));

    for (resourceid_t i = 0; i < 20; i++)
    {
        string data = MakeDataForResource(i);
        MergeTaskResourcePtr resource = newMgr.GetResource(i);
        ASSERT_TRUE(resource);
        ASSERT_EQ(i, resource->GetResourceId());
        ASSERT_EQ(data, string(resource->GetData(), resource->GetDataLen()));
        ASSERT_EQ(StringUtil::toString(i), resource->GetDescription());
    }
}

string MergeTaskResourceManagerTest::MakeDataForResource(resourceid_t resId)
{
    string ret;
    for (resourceid_t i = 0; i < resId + 1; i++)
    {
        ret += StringUtil::toString(i);
    }
    return ret;
}

IE_NAMESPACE_END(index_base);


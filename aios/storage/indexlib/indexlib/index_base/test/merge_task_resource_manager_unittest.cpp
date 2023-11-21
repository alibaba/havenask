#include "indexlib/index_base/test/merge_task_resource_manager_unittest.h"

#include "autil/EnvUtil.h"
#include "autil/StringUtil.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;
using namespace autil;
using namespace indexlib::file_system;
using namespace indexlib::util;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, MergeTaskResourceManagerTest);

MergeTaskResourceManagerTest::MergeTaskResourceManagerTest() {}

MergeTaskResourceManagerTest::~MergeTaskResourceManagerTest() {}

void MergeTaskResourceManagerTest::CaseSetUp() {}

void MergeTaskResourceManagerTest::CaseTearDown() {}

void MergeTaskResourceManagerTest::TestSimpleProcess()
{
    EnvUtil::setEnv("INDEXLIB_PACKAGE_MERGE_META", "true");
    InnerTest(FenceContext::NoFence());
    EnvUtil::unsetEnv("INDEXLIB_PACKAGE_MERGE_META");
    InnerTest(FenceContext::NoFence());
}

void MergeTaskResourceManagerTest::TestFence()
{
    FenceContextPtr fenceContext(FslibWrapper::CreateFenceContext(GET_TEMP_DATA_PATH(), "123"));
    EnvUtil::setEnv("INDEXLIB_PACKAGE_MERGE_META", "true");
    InnerTest(fenceContext.get());
    EnvUtil::unsetEnv("INDEXLIB_PACKAGE_MERGE_META");
    InnerTest(fenceContext.get());
}

void MergeTaskResourceManagerTest::InnerTest(FenceContext* fenceContext)
{
    string rootPath = GET_TEMP_DATA_PATH("merge_resource");
    MergeTaskResourceManager mgr;
    mgr.Init(rootPath, fenceContext);
    for (resourceid_t i = 0; i < 20; i++) {
        string data = MakeDataForResource(i);
        resourceid_t id = mgr.DeclareResource(data.c_str(), data.size(), StringUtil::toString(i));
        ASSERT_EQ(i, id);
    }
    (void)mgr.CreateTempWorkingDirectory();
    auto directory = mgr.GetTempWorkingDirectory();
    auto fileWriter = directory->CreateFileWriter("tmpFile");
    fileWriter->Close().GetOrThrow();
    mgr.Commit();
    string jsonStr = ToJsonString(mgr.GetResourceVec());
    MergeTaskResourceManager::MergeTaskResourceVec resVec;
    FromJsonString(resVec, jsonStr);

    MergeTaskResourceManager newMgr;
    newMgr.Init(resVec);
    ASSERT_TRUE(newMgr.GetTempWorkingDirectory());
    for (resourceid_t i = 0; i < 20; i++) {
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
    for (resourceid_t i = 0; i < resId + 1; i++) {
        ret += StringUtil::toString(i);
    }
    return ret;
}
}} // namespace indexlib::index_base

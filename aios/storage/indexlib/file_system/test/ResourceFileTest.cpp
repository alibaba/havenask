#include "gtest/gtest.h"
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <utility>

#include "autil/CommonMacros.h"
#include "autil/Thread.h"
#include "indexlib/file_system/Directory.h"
#include "indexlib/file_system/ErrorCode.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/FileSystemDefine.h"
#include "indexlib/file_system/FileSystemOptions.h"
#include "indexlib/file_system/MemStorage.h"
#include "indexlib/file_system/StorageMetrics.h"
#include "indexlib/file_system/file/FileNode.h"
#include "indexlib/file_system/file/FileNodeCache.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {
class ExistException;
}} // namespace indexlib::util

using namespace std;
using namespace autil;
using namespace indexlib::util;

namespace indexlib { namespace file_system {

class ResourceFileTest : public INDEXLIB_TESTBASE
{
public:
    ResourceFileTest();
    ~ResourceFileTest();

    DECLARE_CLASS_NAME(ResourceFileTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;

    void TestCreateAndLoadResource();
    void TestResourceHandle();

private:
    std::shared_ptr<Directory> _directory;

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, ResourceFileTest);

INDEXLIB_UNIT_TEST_CASE(ResourceFileTest, TestCreateAndLoadResource);
INDEXLIB_UNIT_TEST_CASE(ResourceFileTest, TestResourceHandle);

//////////////////////////////////////////////////////////////////////

ResourceFileTest::ResourceFileTest() {}

ResourceFileTest::~ResourceFileTest() {}

void ResourceFileTest::CaseSetUp()
{
    _directory = Directory::Get(FileSystemCreator::Create("", GET_TEMP_DATA_PATH()).GetOrThrow());
}

void ResourceFileTest::CaseTearDown() {}

void ResourceFileTest::TestCreateAndLoadResource()
{
    string caseDir = "createload";
    auto dir = _directory->MakeDirectory(caseDir);

    {
        auto resourceFile = dir->CreateResourceFile("resource");
        ASSERT_TRUE(resourceFile->Empty());

        int* res = new int(100);
        resourceFile->Reset(res);
    }
    {
        auto resourceFile = dir->GetResourceFile("resource");
        ASSERT_FALSE(resourceFile->Empty());

        int* data = resourceFile->GetResource<int>();
        ASSERT_TRUE(data);
        ASSERT_EQ(100, *data);
    }
    _directory->RemoveDirectory(caseDir);
    dir = _directory->MakeDirectory(caseDir);
    auto resourceFile = dir->GetResourceFile("resource");
    ASSERT_FALSE(resourceFile);
}

void ResourceFileTest::TestResourceHandle()
{
    string caseDir = "testResourceHandle";
    auto dir = _directory->MakeDirectory(caseDir);
    {
        auto resourceFile = dir->CreateResourceFile("resource");
        ASSERT_TRUE(resourceFile->Empty());

        int* res = new int(100);
        resourceFile->Reset(res);
        ASSERT_FALSE(resourceFile->Empty());
    }
    {
        auto resourceFile = dir->GetResourceFile("resource");
        ASSERT_FALSE(resourceFile->Empty());

        auto handle = resourceFile->GetResourceHandle();
        ASSERT_TRUE(handle);
        int* data = static_cast<int*>(handle.get());
        ASSERT_TRUE(data);
        ASSERT_EQ(100, *data);

        // test old handle will point to the old resource
        resourceFile->Reset(new int(200));
        data = static_cast<int*>(handle.get());
        ASSERT_TRUE(data);
        ASSERT_EQ(100, *data);

        // test next handle will point to the new resource
        handle = resourceFile->GetResourceHandle();
        ASSERT_TRUE(handle);
        data = static_cast<int*>(handle.get());
        ASSERT_TRUE(data);
        ASSERT_EQ(200, *data);
    }
    _directory->RemoveDirectory(caseDir);
    dir = _directory->MakeDirectory(caseDir);
    auto resourceFile = dir->GetResourceFile("resource");
    ASSERT_FALSE(resourceFile);
}

}} // namespace indexlib::file_system

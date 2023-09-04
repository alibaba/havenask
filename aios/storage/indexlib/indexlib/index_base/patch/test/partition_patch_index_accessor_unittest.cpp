#include "indexlib/index_base/patch/test/partition_patch_index_accessor_unittest.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace indexlib::file_system;

namespace indexlib { namespace index_base {
IE_LOG_SETUP(index_base, PartitionPatchIndexAccessorTest);

PartitionPatchIndexAccessorTest::PartitionPatchIndexAccessorTest() {}

PartitionPatchIndexAccessorTest::~PartitionPatchIndexAccessorTest() {}

void PartitionPatchIndexAccessorTest::CaseSetUp() {}

void PartitionPatchIndexAccessorTest::CaseTearDown() {}

void PartitionPatchIndexAccessorTest::TestListPatchRootDirs()
{
    PrepareDirs("patch_index_1,patch_index_3,patch_index_0,patch_index_invalid,abc");
    {
        FileList patchList;
        PartitionPatchIndexAccessor::ListPatchRootDirs(GET_PARTITION_DIRECTORY(), patchList);
        EXPECT_THAT(patchList, UnorderedElementsAre("patch_index_1", "patch_index_3"));
    }
    {
        FileList patchList;
        PartitionPatchIndexAccessor::ListPatchRootDirs(GET_PARTITION_DIRECTORY(), patchList);
        EXPECT_THAT(patchList, UnorderedElementsAre("patch_index_1", "patch_index_3"));
    }
}

void PartitionPatchIndexAccessorTest::PrepareDirs(const string& dirStr)
{
    vector<string> rootDirs;
    StringUtil::fromString(dirStr, rootDirs, ",");

    for (const auto& dir : rootDirs) {
        GET_PARTITION_DIRECTORY()->MakeDirectory(dir);
    }
}
}} // namespace indexlib::index_base

#include "indexlib/common_define.h"
#include "indexlib/file_system/FileSystemCreator.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/index_base/index_meta/parallel_build_info.h"
#include "indexlib/test/unittest.h"

using namespace std;

using namespace indexlib::file_system;
namespace indexlib { namespace index_base {

class ParallelBuildInfoTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(ParallelBuildInfoTest);
    void CaseSetUp() override {}

    void CaseTearDown() override {}

    void TestStore()
    {
        FileSystemOptions fsOptions;
        fsOptions.outputStorage = FSST_PACKAGE_MEM;
        IFileSystemPtr fs = FileSystemCreator::Create("testStore", GET_TEST_DATA_PATH(), fsOptions).GetOrThrow();
        ParallelBuildInfo parallelBuildInfo;
        parallelBuildInfo.StoreIfNotExist(Directory::Get(fs));
        ASSERT_TRUE(
            FslibWrapper::IsExist(GET_TEST_DATA_PATH() + ParallelBuildInfo::PARALLEL_BUILD_INFO_FILE).GetOrThrow());
    }

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index_base, ParallelBuildInfoTest);

INDEXLIB_UNIT_TEST_CASE(ParallelBuildInfoTest, TestStore);
}} // namespace indexlib::index_base

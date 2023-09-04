#include "autil/StringUtil.h"
#include "indexlib/common_define.h"
#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/merger_util/truncate/truncate_index_writer_creator.h"
#include "indexlib/index/test/index_test_util.h"
#include "indexlib/test/unittest.h"

using namespace std;
using namespace autil;

namespace indexlib::index::legacy {
using namespace indexlib::file_system;

class TruncateIndexWriterCreatorTest : public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TruncateIndexWriterCreatorTest);
    void CaseSetUp() override { mRootDir = GET_TEMP_DATA_PATH(); }

    void CaseTearDown() override {}

private:
    string mRootDir;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, TruncateIndexWriterCreatorTest);
} // namespace indexlib::index::legacy

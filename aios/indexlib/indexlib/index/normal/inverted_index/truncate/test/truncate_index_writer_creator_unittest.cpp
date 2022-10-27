#include "indexlib/common_define.h"
#include "indexlib/test/unittest.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_index_writer_creator.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/test/index_test_util.h"
#include <autil/StringUtil.h>

using namespace std;
using namespace autil;

IE_NAMESPACE_BEGIN(index);
IE_NAMESPACE_USE(storage);

class TruncateIndexWriterCreatorTest : 
    public INDEXLIB_TESTBASE
{
public:
    DECLARE_CLASS_NAME(TruncateIndexWriterCreatorTest);
    void CaseSetUp() override
    {
        mRootDir = GET_TEST_DATA_PATH();
    }

    void CaseTearDown() override
    {
    }

private:
    string mRootDir;

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP(index, TruncateIndexWriterCreatorTest);

IE_NAMESPACE_END(index);

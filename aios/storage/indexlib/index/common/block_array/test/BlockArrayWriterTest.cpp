#include "indexlib/index/common/block_array/BlockArrayWriter.h"

#include "indexlib/file_system/file/BufferedFileWriter.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/SimplePool.h"
#include "unittest/unittest.h"

using namespace std;
using namespace indexlib::util;
using namespace indexlib::file_system;
namespace indexlibv2 { namespace index {

class BlockArrayWriterTest : public TESTBASE
{
public:
    BlockArrayWriterTest();
    ~BlockArrayWriterTest();

public:
    void TestSimpleProcess();

public:
    indexlib::util::SimplePool _simplePool;

private:
    AUTIL_LOG_DECLARE();
};

TEST_F(BlockArrayWriterTest, TestSimpleProcess) { TestSimpleProcess(); }
AUTIL_LOG_SETUP(indexlib.index, BlockArrayWriterTest);

BlockArrayWriterTest::BlockArrayWriterTest() {}

BlockArrayWriterTest::~BlockArrayWriterTest() {}

void BlockArrayWriterTest::TestSimpleProcess()
{
    string fileName = PathUtil::JoinPath(GET_TEMP_DATA_PATH(), "test");
    WriterOption option;
    FileWriterPtr writer(new BufferedFileWriter(option));
    ASSERT_EQ(FSEC_OK, writer->Open(fileName, fileName));

    BlockArrayWriter<int, int> blockArrayWriter(&_simplePool);
    ASSERT_TRUE(blockArrayWriter.Init(4096, writer));
    const size_t nKey = 4096 / sizeof(int);
    for (int i = 0; i < nKey; ++i) {
        ASSERT_TRUE(blockArrayWriter.AddItem(i, i).IsOK());
    }
    ASSERT_TRUE(blockArrayWriter.Finish().IsOK());
    ASSERT_EQ(FSEC_OK, writer->Close());
}
}} // namespace indexlibv2::index

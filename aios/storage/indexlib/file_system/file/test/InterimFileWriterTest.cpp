#include "indexlib/file_system/file/InterimFileWriter.h"

#include "gtest/gtest.h"
#include <cstddef>
#include <stdint.h>

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class InterimFileWriterTest : public INDEXLIB_TESTBASE
{
public:
    InterimFileWriterTest();
    ~InterimFileWriterTest();

    DECLARE_CLASS_NAME(InterimFileWriterTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};
AUTIL_LOG_SETUP(indexlib.file_system, InterimFileWriterTest);

INDEXLIB_UNIT_TEST_CASE(InterimFileWriterTest, TestSimpleProcess);
//////////////////////////////////////////////////////////////////////

InterimFileWriterTest::InterimFileWriterTest() {}

InterimFileWriterTest::~InterimFileWriterTest() {}

void InterimFileWriterTest::CaseSetUp() {}

void InterimFileWriterTest::CaseTearDown() {}

void InterimFileWriterTest::TestSimpleProcess()
{
    SCOPED_TRACE("Failed");
    InterimFileWriter writer;
    writer.Init(2);
    ASSERT_EQ(FSEC_OK, writer.Open("test", "test"));
    ASSERT_EQ(FSEC_OK, writer.Write("1", 1).Code());
    char* data = (char*)writer.GetBaseAddress();
    ASSERT_EQ('1', data[0]);
    ASSERT_EQ((uint64_t)1, writer.GetLength());

    ASSERT_EQ(FSEC_OK, writer.Write("234", 3).Code());
    char* newData = (char*)writer.GetBaseAddress();
    ASSERT_FALSE(data == newData);
    ASSERT_EQ((uint64_t)4, writer.GetLength());
    for (size_t i = 0; i < 4; i++) {
        ASSERT_EQ((char)('1' + i), newData[i]);
    }
}
}} // namespace indexlib::file_system

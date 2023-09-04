#include "indexlib/base/Status.h"

#include "autil/Log.h"
#include "unittest/unittest.h"

using namespace std;

namespace indexlib { namespace base {

class StatusTest : public TESTBASE
{
public:
    StatusTest();
    ~StatusTest();

public:
    void setUp() override {}
    void tearDown() override {}
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(indexlib.base, StatusTest);

StatusTest::StatusTest() {}

StatusTest::~StatusTest() {}

TEST_F(StatusTest, TestSimpleProcess)
{
#define ASSERT_STATUS(errorType, codeStr)                                                                              \
    do {                                                                                                               \
        auto s = Status::errorType("test %s %ld", name.c_str(), num);                                                  \
        ASSERT_TRUE(s.Is##errorType());                                                                                \
        ASSERT_EQ(codeStr ": test tablet 5", s.ToString());                                                            \
    } while (0)

    std::string name = "tablet";
    int64_t num = 5;
    ASSERT_STATUS(Corruption, "CORRUPTION");
    ASSERT_STATUS(IOError, "IO_ERROR");
    ASSERT_STATUS(NoMem, "NO_MEMORY");
    ASSERT_STATUS(Unknown, "UNKNOWN");
    ASSERT_STATUS(ConfigError, "CONFIG_ERROR");
    ASSERT_STATUS(InvalidArgs, "INVALID_ARGS");
    ASSERT_STATUS(InternalError, "INTERNAL_ERROR");
    ASSERT_STATUS(NeedDump, "NEED_DUMP");
    ASSERT_STATUS(Unimplement, "UNIMPLEMENT");
    ASSERT_STATUS(OutOfRange, "OUT_OF_RANGE");
    // ASSERT_STATUS(NoEntry, "NO_ENTRY");
    ASSERT_STATUS(Uninitialize, "UNINITIALIZE");
    ASSERT_STATUS(NotFound, "NOT_FOUND");
    ASSERT_STATUS(Expired, "EXPIRED");
    ASSERT_STATUS(Exist, "EXIST");
    ASSERT_STATUS(Sealed, "SEALED");

#undef ASSERT_STATUS
}

}} // namespace indexlib::base

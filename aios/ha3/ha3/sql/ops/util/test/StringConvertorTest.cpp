#include <ha3/test/test.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/util/StringConvertor.h>
#include <unittest/unittest.h>
#include <sstream>

using namespace std;
BEGIN_HA3_NAMESPACE(sql);

class StringConvertorTest : public TESTBASE {
public:
    StringConvertorTest();
    ~StringConvertorTest();
};

StringConvertorTest::StringConvertorTest() {
}

StringConvertorTest::~StringConvertorTest() {
}

#define CHECK(expect)                                                   \
    {                                                                   \
        autil::ConstString value(expect, &pool);                        \
        ASSERT_TRUE(StringConvertor::fromString(&value, val, &pool));   \
        std::stringstream oss;                                          \
        oss << std::setprecision(6) << val;                             \
        EXPECT_EQ(expect, oss.str());                                   \
    }

#define CHECK_DOUBLE(expect)                                            \
    {                                                                   \
        autil::ConstString value(expect, &pool);                        \
        ASSERT_TRUE(StringConvertor::fromString(&value, val, &pool));   \
        std::stringstream oss;                                          \
        oss << std::setprecision(15) << val;                            \
        EXPECT_EQ(expect, oss.str());                                   \
    }

TEST_F(StringConvertorTest, testFromString) {
    autil::mem_pool::Pool pool;
    {
        int val;
        CHECK("123456");
    }
    {
        float val;
        CHECK("23.123");
    }
    {
        double val;
        CHECK_DOUBLE("123456.789123456");
    }
    {
        autil::MultiChar val;
        CHECK("xx");
    }
    {
        autil::MultiChar val;
        CHECK("");
    }
    {
        autil::MultiInt32 val;
        CHECK("1122");
    }
    {
        autil::MultiFloat val;
        CHECK("11.12322.1234");
    }
    {
        autil::MultiDouble val;
        CHECK_DOUBLE("123456.123456789213.4324");
    }
    {
        autil::MultiString val;
        CHECK("");
    }
    {
        autil::MultiString val;
        CHECK("");
    }
    {
        autil::MultiString val;
        CHECK("ab");
    }
    {
        autil::MultiString val;
        CHECK("ac");
    }
    {
        autil::MultiString val;
        CHECK("c");
    }
    {
        autil::MultiString val;
        CHECK("cb");
    }
    {
        autil::MultiString val;
        CHECK("");
    }
}

END_HA3_NAMESPACE(sql);

#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/tvf/TvfResourceBase.h"

using namespace std;
using namespace testing;

BEGIN_HA3_NAMESPACE(sql);

class TvfResourceBaseTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
private:
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, TvfResourceBaseTest);

void TvfResourceBaseTest::setUp() {
}

void TvfResourceBaseTest::tearDown() {
}
class UnknownTvfResource : public TvfResourceBase {
public:
    string name() const override {
        return "UnknownResource";
    }
};

class MyTvfResource : public TvfResourceBase {
public:
    MyTvfResource()
        : count (0)
    {}
    string name() const override {
        return "MyTvfResource";
    }
public:
    int32_t count = 0;
};

TEST_F(TvfResourceBaseTest, testPutAndGetTvfResource) {
    TvfResourceContainer container;
    MyTvfResource *res = container.get<MyTvfResource>("MyTvfResource");
    ASSERT_TRUE(res == nullptr);
    MyTvfResource *resource = new MyTvfResource();
    resource->count = 10;
    ASSERT_TRUE(container.put(resource));
    ASSERT_FALSE(container.put(resource));

    res = container.get<MyTvfResource>("MyTvfResource");
    ASSERT_TRUE(res != nullptr);
    ASSERT_EQ(10, res->count);
    ASSERT_TRUE(container.get<UnknownTvfResource>("MyTvfResource") == nullptr);
}

END_HA3_NAMESPACE();

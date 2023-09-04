#include "sql/ops/scan/udf_to_query/UdfToQueryManager.h"

#include <cstddef>
#include <memory>

#include "sql/common/common.h"
#include "sql/ops/scan/udf_to_query/UdfToQueryCreatorFactory.h"
#include "unittest/unittest.h"

namespace autil {
class InterfaceManager;
} // namespace autil

using namespace std;

namespace sql {
class UdfToQueryManagerTest : public TESTBASE {
public:
    UdfToQueryManagerTest();
    ~UdfToQueryManagerTest();

public:
    void setUp();
    void tearDown();
};

UdfToQueryManagerTest::UdfToQueryManagerTest() {}

UdfToQueryManagerTest::~UdfToQueryManagerTest() {}

void UdfToQueryManagerTest::setUp() {}

void UdfToQueryManagerTest::tearDown() {}

class FakeFactory : public UdfToQueryCreatorFactory {
    bool registerUdfToQuery(autil::InterfaceManager *manager) override {
        return false;
    }
};

TEST_F(UdfToQueryManagerTest, testFind) {
    UdfToQueryManager udfToQueryManager;
    ASSERT_TRUE(udfToQueryManager.init());
    ASSERT_TRUE(udfToQueryManager.find(SQL_UDF_MATCH_OP));
    ASSERT_FALSE(udfToQueryManager.find("not_exist"));
    UdfToQueryCreatorFactoryPtr fakeFactory(new FakeFactory());
    ASSERT_FALSE(fakeFactory->registerUdfToQuery(&udfToQueryManager));
}

} // namespace sql

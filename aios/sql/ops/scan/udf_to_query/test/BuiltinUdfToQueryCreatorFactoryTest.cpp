#include "sql/ops/scan/udf_to_query/BuiltinUdfToQueryCreatorFactory.h"

#include <cstddef>

#include "autil/plugin/InterfaceManager.h"
#include "unittest/unittest.h"

using namespace std;

namespace sql {
class BuiltinUdfToQueryCreatorFactoryTest : public TESTBASE {
public:
    BuiltinUdfToQueryCreatorFactoryTest();
    ~BuiltinUdfToQueryCreatorFactoryTest();

public:
    void setUp();
    void tearDown();
};

BuiltinUdfToQueryCreatorFactoryTest::BuiltinUdfToQueryCreatorFactoryTest() {}

BuiltinUdfToQueryCreatorFactoryTest::~BuiltinUdfToQueryCreatorFactoryTest() {}

void BuiltinUdfToQueryCreatorFactoryTest::setUp() {}

void BuiltinUdfToQueryCreatorFactoryTest::tearDown() {}

TEST_F(BuiltinUdfToQueryCreatorFactoryTest, testRegisterUdfToQuery) {
    autil::InterfaceManager manager;
    BuiltinUdfToQueryCreatorFactory factory;
    factory.registerUdfToQuery(&manager);
    ASSERT_TRUE(manager.find("MATCHINDEX") != nullptr);
    ASSERT_TRUE(manager.find("QUERY") != nullptr);
    ASSERT_TRUE(manager.find("contain") != nullptr);
    ASSERT_TRUE(manager.find("ha_in") != nullptr);
    ASSERT_TRUE(manager.find("pidvid_contain") != nullptr);
    ASSERT_TRUE(manager.find("sp_query_match") != nullptr);
}

} // namespace sql

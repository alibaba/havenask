#include "build_service/config/ProcessorChainSelectorConfig.h"

#include <iostream>
#include <map>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace config {

class ProcessorChainSelectorConfigTest : public BUILD_SERVICE_TESTBASE
{
public:
    void setUp();
    void tearDown();
};

void ProcessorChainSelectorConfigTest::setUp() {}

void ProcessorChainSelectorConfigTest::tearDown() {}

TEST_F(ProcessorChainSelectorConfigTest, testJsonize)
{
    ProcessorChainSelectorConfig config;
    SelectRule rule1;
    rule1.matcher.insert(make_pair("field1", "value1"));
    rule1.matcher.insert(make_pair("field2", "value2"));
    rule1.destChains.push_back("chain1");
    config.selectRules.push_back(rule1);
    SelectRule rule2;
    rule2.matcher.insert(make_pair("field1", "value3"));
    rule2.matcher.insert(make_pair("field2", "value4"));
    rule2.destChains.push_back("chain2");
    rule2.destChains.push_back("chain3");
    config.selectRules.push_back(rule2);

    config.selectFields.push_back("field1");
    config.selectFields.push_back("field2");

    string str = ToJsonString(config);
    cout << str << endl;
    ProcessorChainSelectorConfig other;
    ASSERT_NO_THROW(FromJsonString(other, str));

    EXPECT_EQ(config.selectRules, other.selectRules);
    EXPECT_EQ(config.selectFields, other.selectFields);
}

}} // namespace build_service::config

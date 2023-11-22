#include "build_service/config/GenerationMeta.h"

#include <iosfwd>
#include <string>

#include "build_service/test/unittest.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace build_service { namespace config {

class GenerationMetaTest : public BUILD_SERVICE_TESTBASE
{
};

TEST_F(GenerationMetaTest, testSimpleProcess)
{
    string indexRoot = GET_TEST_DATA_PATH() + "generation_meta_test/indexroot1";
    GenerationMeta gm;
    EXPECT_TRUE(gm.loadFromFile(indexRoot));
    EXPECT_EQ(string("value1"), gm.getValue("key1"));
    EXPECT_EQ(string(""), gm.getValue("key2"));
    EXPECT_EQ(string("value3"), gm.getValue("key3"));

    // compatible mode, in generation dir
    GenerationMeta gm2;
    indexRoot = GET_TEST_DATA_PATH() + "generation_meta_test/indexroot2/partition_dir";
    EXPECT_TRUE(gm2.loadFromFile(indexRoot));
    EXPECT_EQ(string("value"), gm2.getValue("key"));
    EXPECT_EQ(string(""), gm2.getValue("key1"));
    EXPECT_EQ(string("value2"), gm2.getValue("key2"));

    indexRoot = GET_TEST_DATA_PATH() + "generation_meta_test/indexroot3";
    EXPECT_FALSE(gm2.loadFromFile(indexRoot));
}

}} // namespace build_service::config

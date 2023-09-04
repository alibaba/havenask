#include "swift/config/ConfigReader.h"

#include <iosfwd>
#include <map>
#include <stdint.h>
#include <string>

#include "autil/Log.h"
#include "unittest/unittest.h"

namespace swift {
namespace config {

class ConfigReaderTest : public TESTBASE {
public:
    ConfigReaderTest();
    ~ConfigReaderTest();

public:
    void setUp();
    void tearDown();

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(swift, ConfigReaderTest);

using namespace std;
using namespace swift::config;

ConfigReaderTest::ConfigReaderTest() {}

ConfigReaderTest::~ConfigReaderTest() {}

void ConfigReaderTest::setUp() { AUTIL_LOG(DEBUG, "setUp!"); }

void ConfigReaderTest::tearDown() { AUTIL_LOG(DEBUG, "tearDown!"); }

TEST_F(ConfigReaderTest, testRead) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader confReader;
    bool flag = confReader.read(filePath);
    ASSERT_TRUE(flag);
}

TEST_F(ConfigReaderTest, testHasSection) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader cr;
    bool flag = cr.read(filePath);
    ASSERT_TRUE(flag);
    flag = cr.hasSection("broker");
    ASSERT_TRUE(flag);
    flag = cr.hasSection("brokers");
    ASSERT_TRUE(!flag);
}

TEST_F(ConfigReaderTest, testHasOption) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader cr;
    bool flag = cr.read(filePath);
    ASSERT_TRUE(flag);
    flag = cr.hasOption("broker", "thread_num");
    ASSERT_TRUE(flag);
    flag = cr.hasOption("broker", "thread_nums");
    ASSERT_TRUE(!flag);
    flag = cr.hasOption("brokers", "thread_nums");
    ASSERT_TRUE(!flag);
}

TEST_F(ConfigReaderTest, testGet) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader cr;
    bool flag = cr.read(filePath);
    ASSERT_TRUE(flag);
    string bindAddress = cr.get("broker", "bind_address", "/root/");
    ASSERT_TRUE((bindAddress == "tcp:0.0.0.0:11111"));
    bindAddress = cr.get("broker", "bind_addresses", "/root/");
    ASSERT_TRUE((bindAddress == "/root/"));
}

TEST_F(ConfigReaderTest, testGetInt32) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader cr;
    bool flag = cr.read(filePath);
    ASSERT_TRUE(flag);
    int32_t queueSize;
    queueSize = cr.getInt32("broker", "queue_size", -2);
    ASSERT_EQ(1000, queueSize);
    queueSize = cr.getInt32("broker", "queue_sizes", -2);
    ASSERT_EQ(-2, queueSize);
}

TEST_F(ConfigReaderTest, testGetUint32) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader cr;
    bool flag = cr.read(filePath);
    ASSERT_TRUE(flag);
    uint32_t dpbs;
    dpbs = cr.getUint32("broker", "default_partition_buffer_size", 100);
    ASSERT_TRUE(102410240 == dpbs);
    dpbs = cr.getUint32("broker", "default_partition_buffer_sizes", 1000);
    ASSERT_TRUE(1000 == dpbs);
}

TEST_F(ConfigReaderTest, testGetDouble) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader cr;
    bool flag = cr.read(filePath);
    ASSERT_TRUE(flag);
    double ratio;
    ratio = cr.getDouble("broker", "ratio", 100.00);
    ASSERT_TRUE((ratio - 234.567) < 0.001);
    ratio = cr.getDouble("broker", "ratios", 100.00);
    ASSERT_TRUE((ratio - 100.00) < 0.01);
}

TEST_F(ConfigReaderTest, testGetBool) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader cr;
    bool flag = cr.read(filePath);
    ASSERT_TRUE(flag);
    bool readable;
    readable = cr.getBool("broker", "dfsReadable", false);
    ASSERT_TRUE(readable);
    readable = cr.getBool("broker", "dfsReadables", true);
    ASSERT_TRUE(readable);
}

TEST_F(ConfigReaderTest, testItems) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader cr;
    bool flag = cr.read(filePath);
    ASSERT_TRUE(flag);
    ConfigReader::Key2ValMap kvMap = cr.items("broker");
    ASSERT_TRUE(6 == kvMap.size());
    kvMap = cr.items("brokers");
    ASSERT_TRUE(0 == kvMap.size());
}

TEST_F(ConfigReaderTest, testSimpleProcess) {
    AUTIL_LOG(DEBUG, "Begin Test!");

    // ASSERT_TRUE(false);
    // ASSERT_EQ(0, 1);
}

TEST_F(ConfigReaderTest, testGetKVByPrefix) {
    string filePath = GET_TEST_DATA_PATH() + "config/swift.conf";
    ConfigReader cr;
    bool flag = cr.read(filePath);
    ASSERT_TRUE(flag);
    std::map<std::string, std::string> kvMap = cr.getKVByPrefix("kv_test", "testPrefix_");
    ASSERT_EQ(2, kvMap.size());
    ASSERT_EQ("value1", kvMap["testPrefix_key1"]);
    ASSERT_EQ("", kvMap["testPrefix_key2"]);

    std::map<std::string, std::string> kvMap2 = cr.getKVByPrefix("broker", "testPrefix_");
    ASSERT_EQ(0, kvMap2.size());
}

} // namespace config
} // namespace swift

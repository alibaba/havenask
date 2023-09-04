#include "swift/tools/config_validator/ConfigValidator.h"

#include <algorithm>
#include <cstddef>
#include <ext/alloc_traits.h>
#include <string>
#include <vector>

#include "fslib/common/common_type.h"
#include "fslib/fs/FileSystem.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace swift {
namespace tools {

class ConfigValidatorTest : public TESTBASE {
public:
    void setUp() {
        _srcConfigPath = GET_TEST_DATA_PATH() + "config_validator_test/";
        _configPath = GET_TEMP_DATA_PATH() + "/";
        _configs.push_back("swift_hippo.json");
        _configs.push_back("swift.conf");
    }
    void generateConfig(const string &invalidConfig) {
        for (size_t i = 0; i < _configs.size(); i++) {
            string content;
            ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::readFile(_srcConfigPath + _configs[i], content));
            if (_configs[i] == invalidConfig) {
                ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::writeFile(_configPath + _configs[i], "invalid json"));
            } else {
                ASSERT_EQ(fslib::EC_OK, fslib::fs::FileSystem::writeFile(_configPath + _configs[i], content));
            }
        }
    }

public:
    string _srcConfigPath;
    string _configPath;
    vector<string> _configs;
};

TEST_F(ConfigValidatorTest, testSimple) {
    // make sure all configs validated
    for (size_t i = 0; i < _configs.size(); i++) {
        ConfigValidator validator;
        generateConfig(_configs[i]);
        ASSERT_FALSE(validator.validate(_configPath)) << _configs[i];
    }
    ConfigValidator validator;
    generateConfig("");
    ASSERT_TRUE(validator.validate(_configPath));
}

}; // namespace tools
}; // namespace swift

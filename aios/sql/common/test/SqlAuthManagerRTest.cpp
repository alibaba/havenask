#include "sql/common/SqlAuthManagerR.h"

#include <iosfwd>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>

#include "autil/legacy/exception.h"
#include "autil/legacy/md5.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/config/AuthenticationConfig.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace autil::legacy;

namespace sql {

class SqlAuthManagerRTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;
};

void SqlAuthManagerRTest::setUp() {}

void SqlAuthManagerRTest::tearDown() {}

TEST_F(SqlAuthManagerRTest, testItemSign) {
    std::string token = "token";
    std::string secret = "74b87337454200d4d33f80c4663dc5e5";
    // sql?query=update biz_order_summary set status = ? where biz_order_id=
    // ?&&kvpair=databaseName:biz_order_summary;timeout:1000;dynamic_params:[["999","1302461072256853"]]
    std::string queryStr
        = R"query(update biz_order_summary set status = ? where biz_order_id= ?)query";
    std::string kvPairStr
        = R"kvPair(databaseName:biz_order_summary;timeout:1000;dynamic_params:[["999","1302461072256853"]])kvPair";
    SqlAuthItem item(secret);
    std::string payload = queryStr + kvPairStr + secret;
    Md5Stream stream;
    stream.Put((const uint8_t *)payload.c_str(), payload.length());
    ASSERT_EQ(stream.GetMd5String(), item.sign(queryStr, kvPairStr));
}

TEST_F(SqlAuthManagerRTest, testInit) {
    AuthenticationConfig config;
    config.tokenPairs.emplace("token1", "secret1");
    config.tokenPairs.emplace("token2", "secret2");
    SqlAuthManagerR managerR;
    ASSERT_TRUE(managerR.init(config));
    auto &map = managerR._authMap;
    ASSERT_EQ(2, map.size());
    {
        auto it = map.find("token1");
        ASSERT_NE(map.end(), it);
        ASSERT_EQ("secret1", it->second._secret);
    }
    {
        auto it = map.find("token2");
        ASSERT_NE(map.end(), it);
        ASSERT_EQ("secret2", it->second._secret);
    }
}

TEST_F(SqlAuthManagerRTest, testGetAuthItem) {
    SqlAuthManagerR managerR;
    managerR._authMap = {
        {"token1", {"secret1"}},
        {"token2", {"secret2"}},
    };
    ASSERT_NE(nullptr, managerR.getAuthItem("token1"));
    ASSERT_NE(nullptr, managerR.getAuthItem("token2"));
    ASSERT_EQ(nullptr, managerR.getAuthItem("token_not_found"));
}

TEST_F(SqlAuthManagerRTest, testDisable) {
    std::string configStr = R"json({
        "authentication_config": {
            "enable": false,
            "token_pairs": {
                "token1": "secret1",
                "token2": "secret2"
            }
        }
    })json";
    navi::NaviResourceHelper naviRHelper;
    naviRHelper.kernelConfig(configStr);
    auto managerR = naviRHelper.getOrCreateResPtr<SqlAuthManagerR>();
    ASSERT_TRUE(managerR);
    ASSERT_TRUE(managerR->_authMap.empty());
}

TEST_F(SqlAuthManagerRTest, testEnable) {
    std::string configStr = R"json({
        "authentication_config": {
            "enable": true,
            "token_pairs": {
                "token1": "secret1",
                "token2": "secret2"
            }
        }
    })json";
    navi::NaviResourceHelper naviRHelper;
    naviRHelper.config(SqlAuthManagerR::RESOURCE_ID, configStr);
    auto managerR = naviRHelper.getOrCreateResPtr<SqlAuthManagerR>();
    ASSERT_TRUE(managerR);
    ASSERT_EQ(2, managerR->_authMap.size());
    ASSERT_EQ("secret1", managerR->_authMap["token1"]._secret);
    ASSERT_EQ("secret2", managerR->_authMap["token2"]._secret);
}

} // end namespace sql

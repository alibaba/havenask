#include <iosfwd>
#include <map>
#include <string>
#include <utility>

#include "swift/config/AuthorizerInfo.h"
#include "swift/config/AuthorizerParser.h"
#include "swift/config/ConfigReader.h"
#include "swift/protocol/Common.pb.h"
#include "unittest/unittest.h"

namespace swift {
namespace config {

class AuthorizerParserTest : public TESTBASE {};

using namespace swift::protocol;
using namespace std;

TEST_F(AuthorizerParserTest, testParseAuthentications) {
    {
        string confPath = GET_TEST_DATA_PATH() + "config/config_authentication_test/swift_authentication.conf";
        config::ConfigReader reader;
        EXPECT_TRUE(reader.read(confPath));
        AuthorizerInfo conf;
        AuthorizerParser parser;
        EXPECT_EQ(false, conf._enable);
        EXPECT_TRUE(parser.parseAuthorizer(reader, conf));
        EXPECT_EQ(true, conf._enable);
        std::map<std::string, std::string> kvMap1 = {{"az19-[];'./,\\*-+", "~!@#$%^&*()_+{}|:\"<>?ia01"},
                                                     {"abc", "abc1"}};
        std::map<std::string, std::string> kvMap2 = {{"test1", "key1"}, {"test2", "key2"}};
        ASSERT_EQ(kvMap1, conf._sysUsers);
        ASSERT_EQ(kvMap2, conf._normalUsers);
        ASSERT_EQ("abc", conf._innerSysUser);
        string username, passwd;
        conf.getInnerSysUserPasswd(username, passwd);
        ASSERT_EQ("abc", username);
        ASSERT_EQ("abc1", passwd);
    }
    {
        string confPath = GET_TEST_DATA_PATH() + "config/config_authentication_test/swift_authentication_disable.conf";
        config::ConfigReader reader;
        EXPECT_TRUE(reader.read(confPath));
        AuthorizerInfo conf;
        AuthorizerParser parser;
        EXPECT_EQ(false, conf._enable);
        EXPECT_TRUE(parser.parseAuthorizer(reader, conf));
        EXPECT_EQ(false, conf._enable);
        std::map<std::string, std::string> kvMap1 = {{"az19-[];'./,\\*-+", "~!@#$%^&*()_+{}|:\"<>?ia01"}};
        std::map<std::string, std::string> kvMap2 = {{"test1", "key1"}, {"test2", "key2"}};
        ASSERT_EQ(kvMap1, conf._sysUsers);
        ASSERT_EQ(kvMap2, conf._normalUsers);
        ASSERT_EQ("", conf._innerSysUser);
        string username, passwd;
        conf.getInnerSysUserPasswd(username, passwd);
        ASSERT_EQ(kvMap1.begin()->first, username);
        ASSERT_EQ(kvMap1.begin()->second, passwd);
    }
    {
        string confPath = GET_TEST_DATA_PATH() + "config/config_authentication_test/swift_authentication_none.conf";
        config::ConfigReader reader;
        EXPECT_TRUE(reader.read(confPath));
        AuthorizerInfo conf;
        AuthorizerParser parser;
        EXPECT_EQ(false, conf._enable);
        EXPECT_TRUE(parser.parseAuthorizer(reader, conf));
        EXPECT_EQ(false, conf._enable);
        std::map<std::string, std::string> kvMap1 = {{"az19-[];'./,\\*-+", "~!@#$%^&*()_+{}|:\"<>?ia01"}};
        std::map<std::string, std::string> kvMap2 = {{"test1", "key1"}, {"test2", "key2"}};
        ASSERT_EQ(kvMap1, conf._sysUsers);
        ASSERT_EQ(kvMap2, conf._normalUsers);
    }
    {
        string confPath = GET_TEST_DATA_PATH() + "config/config_authentication_test/swift_authentication_error.conf";
        config::ConfigReader reader;
        EXPECT_TRUE(reader.read(confPath));
        AuthorizerInfo conf;
        AuthorizerParser parser;
        EXPECT_EQ(false, conf._enable);
        EXPECT_FALSE(parser.parseAuthorizer(reader, conf));
        EXPECT_EQ(false, conf._enable);
        EXPECT_EQ(0, conf._sysUsers.size());
        EXPECT_EQ(0, conf._normalUsers.size());
    }
}

TEST_F(AuthorizerParserTest, testValidate) {
    AuthorizerParser parser;
    EXPECT_TRUE(parser.validate(false, {}, {}));
    EXPECT_FALSE(parser.validate(true, {}, {}));

    EXPECT_TRUE(parser.validate(true, {{"user1", "key1"}}, {}));
    EXPECT_TRUE(parser.validate(true, {{"user1", "key1"}}, {{"user2", "key2"}}));
    EXPECT_FALSE(parser.validate(true, {{"user1", "key1"}}, {{"user1", "key1"}}));
}

} // namespace config
} // namespace swift

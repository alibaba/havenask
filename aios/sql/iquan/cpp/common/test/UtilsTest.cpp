#include "iquan/common/Utils.h"

#include <map>
#include <string>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "iquan/common/Common.h"
#include "iquan/common/IquanException.h"
#include "iquan/common/Status.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class UtilsTest : public TESTBASE {};

TEST_F(UtilsTest, testAnyToString) {
    {
        autil::legacy::Any any = autil::legacy::Any(true);
        std::string typeStr = Utils::anyToString(any, "boolean");
        ASSERT_EQ("true", typeStr);
    }

    {
        autil::legacy::Any any = autil::legacy::Any(std::string("str"));
        {
            std::string typeStr = Utils::anyToString(any, "varchar");
            ASSERT_EQ("\"str\"", typeStr);
        }

        {
            std::string typeStr = Utils::anyToString(any, "char");
            ASSERT_EQ("\"str\"", typeStr);
        }

        {
            std::string typeStr = Utils::anyToString(any, "string");
            ASSERT_EQ("\"str\"", typeStr);
        }
    }

    {
        autil::legacy::Any any = autil::legacy::Any(123);
        std::string typeStr = Utils::anyToString(any, "int32");
        ASSERT_EQ("123", typeStr);
    }

    {
        autil::legacy::Any any = autil::legacy::Any(double(1.123));
        std::string typeStr = Utils::anyToString(any, "double");
        ASSERT_EQ("1.123", typeStr);
    }
}

TEST_F(UtilsTest, testReadValue) {
    autil::legacy::json::JsonMap map;
    map[std::string("aa")] = autil::legacy::Any(std::string("abc"));
    map[std::string("bb")] = autil::legacy::Any(int(123));
    map[std::string("cc")] = autil::legacy::Any(double(1.123));

    {
        std::string value;
        Status status = Utils::readValue(map, std::string("aa"), value);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_EQ(std::string("abc"), value);
    }

    {
        int value;
        Status status = Utils::readValue(map, std::string("bb"), value);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_EQ(int(123), value);
    }

    {
        double value;
        Status status = Utils::readValue(map, std::string("cc"), value);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_DOUBLE_EQ(double(1.123), value);
    }

    {
        std::string value;
        Status status = Utils::readValue(map, "dd", value);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ(int(IQUAN_CACHE_KEY_NOT_FOUND), status.code());
    }

    {
        std::string value;
        Status status = Utils::readValue(map, "bb", value);
        ASSERT_FALSE(status.ok());
        ASSERT_EQ(int(IQUAN_BAD_ANY_CAST), status.code());
    }
}

TEST_F(UtilsTest, testReadFiles) {
    std::vector<std::string> pathList
        = {GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/a"),
           GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/not_exist"),
           GET_PRIVATE_TEST_DATA_PATH_WITHIN_TEST() + std::string("/b")};
    std::vector<std::string> fileContentList;
    { ASSERT_FALSE(Utils::readFiles(pathList, fileContentList, false, false).ok()); }
    {
        ASSERT_TRUE(Utils::readFiles(pathList, fileContentList, false, true).ok());
        ASSERT_EQ(4, fileContentList.size());
        std::vector<std::string> expected = {"1", "2", "3", "4"};
        ASSERT_EQ(expected, fileContentList);
    }
    {
        ASSERT_TRUE(Utils::readFiles(pathList, fileContentList, true, true).ok());
        ASSERT_EQ(4, fileContentList.size());
        std::vector<std::string> expected = {"2", "1", "4", "3"};
        ASSERT_EQ(expected, fileContentList);
    }
}

} // namespace iquan

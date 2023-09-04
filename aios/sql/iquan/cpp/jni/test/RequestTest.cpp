#include "autil/Log.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "iquan/common/Common.h"
#include "iquan/common/Status.h"
#include "iquan/common/Utils.h"
#include "iquan/jni/IquanDqlRequest.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class RequestTest : public TESTBASE {
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, RequestTest);

TEST_F(RequestTest, testIquanDqlRequest) {
    IquanDqlRequest queryRequest;
    ASSERT_FALSE(queryRequest.isValid());
    ASSERT_EQ(0, queryRequest.sqls.size());
    ASSERT_EQ(0, queryRequest.sqlParams.size());
    ASSERT_TRUE(queryRequest.dynamicParams.empty());
    ASSERT_EQ("", queryRequest.defaultCatalogName);
    ASSERT_EQ("", queryRequest.defaultDatabaseName);

    queryRequest.sqls.push_back("select * from t1");
    ASSERT_FALSE(queryRequest.isValid());
    queryRequest.sqlParams["key"] = std::string("value");
    ASSERT_FALSE(queryRequest.isValid());
    queryRequest.defaultCatalogName = "test";
    ASSERT_FALSE(queryRequest.isValid());
    queryRequest.defaultDatabaseName = "phone";
    ASSERT_TRUE(queryRequest.isValid());

    // not contain dynamic params
    {
        std::string actualStr;
        std::string expectedStr = R"json({
"default_catalog_name":
  "test",
"default_database_name":
  "phone",
"dynamic_params":
  [
  ],
"sql_params":
  {
  "key":
    "value"
  },
"sqls":
  [
    "select * from t1"
  ]
})json";

        Status status = Utils::toJson(queryRequest, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

        IquanDqlRequest queryRequest2;
        status = Utils::fromJson(queryRequest2, actualStr);
        ASSERT_TRUE(status.ok());
    }

    // contain dynamic params & sql params
    {
        std::string actualStr;
        std::string expectedStr = R"json({
"default_catalog_name":
  "test",
"default_database_name":
  "phone",
"dynamic_params":
  [
    [
      1,
      1.123,
      "test"
    ]
  ],
"sql_params":
  {
  "key":
    "value"
  },
"sqls":
  [
    "select * from t1"
  ]
})json";
        std::vector<autil::legacy::Any> dynamicParams;
        dynamicParams.emplace_back(autil::legacy::Any(1));
        dynamicParams.emplace_back(autil::legacy::Any(1.123));
        dynamicParams.emplace_back(autil::legacy::Any(std::string("test")));

        queryRequest.dynamicParams._array.emplace_back(dynamicParams);
        ASSERT_TRUE(queryRequest.isValid());

        Status status = Utils::toJson(queryRequest, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

        IquanDqlRequest queryRequest2;
        status = Utils::fromJson(queryRequest2, actualStr);
        ASSERT_TRUE(status.ok());

        // test sql params
        queryRequest2.sqlParams["key1"] = 1;
        queryRequest2.sqlParams["key2"] = 1.123;
        queryRequest2.sqlParams["key3"] = autil::legacy::Any(std::string("test"));
        queryRequest2.sqlParams["key4"] = true;
        ASSERT_TRUE(queryRequest2.isValid());

        expectedStr = R"json({
"default_catalog_name":
  "test",
"default_database_name":
  "phone",
"dynamic_params":
  [
    [
      1,
      1.123,
      "test"
    ]
  ],
"sql_params":
  {
  "key":
    "value",
  "key1":
    1,
  "key2":
    1.123,
  "key3":
    "test",
  "key4":
    true
  },
"sqls":
  [
    "select * from t1"
  ]
})json";
        status = Utils::toJson(queryRequest2, actualStr, false);
        ASSERT_TRUE(status.ok()) << status.errorMessage();
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectedStr, actualStr));

        IquanDqlRequest queryRequest3;
        status = Utils::fromJson(queryRequest3, actualStr);
        ASSERT_TRUE(status.ok());

        // if sql.size != dynamicParams.size()
        queryRequest3.dynamicParams._array.emplace_back(dynamicParams);
        ASSERT_FALSE(queryRequest3.isValid());
    }
}

TEST_F(RequestTest, testToJson) {
    IquanDqlRequest request;
    request.sqls.emplace_back("select * from t1 where i1>?");
    request.sqlParams["key1"] = 1;
    std::vector<autil::legacy::Any> dynamicParams;
    dynamicParams.emplace_back(autil::legacy::Any(1));
    request.dynamicParams._array.emplace_back(dynamicParams);
    request.defaultCatalogName = "default";
    request.defaultDatabaseName = "db1";
    request.execParams["exec_key"] = "xxx";
    request.queryHashKey = 123;

    {
        std::string jsonStr;
        auto status = request.toWarmupJson(jsonStr);
        EXPECT_TRUE(status.ok()) << status.errorMessage();
        std::string expectJsonStr = "{\"default_catalog_name\":\"default\",\"default_database_"
                                    "name\":\"db1\",\"query_hash_key\":123,\"sql_params\":{"
                                    "\"key1\":1},\"sqls\":[\"select * from t1 where i1>?\"]}";
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectJsonStr, jsonStr));
    }
    {
        std::string jsonStr;
        auto status = request.toCacheKey(jsonStr);
        EXPECT_TRUE(status.ok()) << status.errorMessage();
        std::string expectJsonStr = "{\"sqls\":[\"select * from t1 where "
                                    "i1>?\"],\"sql_params\":{\"key1\":1},\"default_catalog_name\":"
                                    "\"default\",\"default_database_name\":\"db1\"}";
        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectJsonStr, jsonStr));
    }
}

TEST_F(RequestTest, testFromWarmup) {
    std::string jsonStr = R"json({
"default_catalog_name":
  "test",
"default_database_name":
  "phone",
"dynamic_params":
  [
    [
      1,
      1.123,
      "test"
    ]
  ],
"sql_params":
  {
  "key":
    "value"
  },
"sqls":
  [
    "select * from t1"
  ],
"query_hash_key":
  123
})json";
    IquanDqlRequest request;
    auto status = request.fromWarmupJson(jsonStr);
    EXPECT_TRUE(status.ok()) << status.errorMessage();
    ASSERT_EQ(1, request.sqls.size());
    EXPECT_EQ("select * from t1", request.sqls[0]);
    EXPECT_EQ(1, request.sqlParams.size());
    EXPECT_TRUE(request.dynamicParams._array.empty());
    EXPECT_EQ("test", request.defaultCatalogName);
    EXPECT_EQ("phone", request.defaultDatabaseName);
    EXPECT_TRUE(request.execParams.empty());
    EXPECT_EQ(123UL, request.queryHashKey);
}

} // namespace iquan

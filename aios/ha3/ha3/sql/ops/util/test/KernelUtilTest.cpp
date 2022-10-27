#include <ha3/test/test.h>
#include <ha3/sql/common/common.h>
#include <ha3/sql/ops/util/KernelUtil.h>
#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;

BEGIN_HA3_NAMESPACE(sql);

class KernelUtilTest : public OpTestBase {
public:
    KernelUtilTest();
    ~KernelUtilTest();
public:
    void setUp() override {
    }
    void tearDown() override {
    }
};

KernelUtilTest::KernelUtilTest() {
}

KernelUtilTest::~KernelUtilTest() {
}

TEST_F(KernelUtilTest, testGetPool) {
    {
        KernelInitContext ctx(_resourceMap);
        auto pool = KernelUtil::getPool(ctx);
        ASSERT_TRUE(pool != nullptr);
    }
    {
        KernelInitContext ctx({});
        auto pool = KernelUtil::getPool(ctx);
        ASSERT_TRUE(pool == nullptr);
    }
}


TEST_F(KernelUtilTest, testStripName) {
    map<string, vector<string> > value = { { "$name", { "$a", "$b" } } };
    KernelUtil::stripName(value);
    const auto &it = value.find("name");
    ASSERT_TRUE(it != value.end());
    ASSERT_EQ(2, it->second.size());
    ASSERT_EQ("a", it->second[0]);
    ASSERT_EQ("b", it->second[1]);
}

TEST_F(KernelUtilTest, testToJsonString) {
    string jsonStr = R"json({"hash_mode":{"hash_function":"HASH","hash_fields":["$attr1"]}, "other":1})json";
    {
        RapidDocument document;
        autil_rapidjson::ParseResult ok = document.Parse(jsonStr.c_str());
        ASSERT_TRUE(ok);
        autil::legacy::Jsonizable::JsonWrapper wrapper(&document);
        string output;
        ASSERT_TRUE(KernelUtil::toJsonString(wrapper, "no_exist", output));
        ASSERT_EQ("", output);
        output = "";
        ASSERT_TRUE(KernelUtil::toJsonString(wrapper, "other", output));
        ASSERT_EQ("1", output);
        output = "";
        ASSERT_TRUE(KernelUtil::toJsonString(wrapper, "hash_mode", output));
        ASSERT_EQ("{\"hash_function\":\"HASH\",\"hash_fields\":[\"$attr1\"]}", output);
    }
    {
        Any any;
        FromJsonString(any, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(any);
        string output;
        ASSERT_TRUE(KernelUtil::toJsonString(wrapper, "no_exist", output));
        ASSERT_EQ("", output);
        output = "";
        ASSERT_TRUE(KernelUtil::toJsonString(wrapper, "other", output));
        ASSERT_EQ("1", output);
        output = "";
        ASSERT_TRUE(KernelUtil::toJsonString(wrapper, "hash_mode", output));
        ASSERT_EQ("{\"hash_fields\":[\"$attr1\"],\"hash_function\":\"HASH\"}", output);
    }

}

TEST_F(KernelUtilTest, testToJsonWrapper) {
    string jsonStr = R"json({"hash_mode":{"hash_function":"HASH","hash_fields":["$attr1"]}, "other":1})json";
    {
        RapidDocument document;
        autil_rapidjson::ParseResult ok = document.Parse(jsonStr.c_str());
        ASSERT_TRUE(ok);
        autil::legacy::Jsonizable::JsonWrapper wrapper(&document);
        autil::legacy::Jsonizable::JsonWrapper outWrapper;
        ASSERT_FALSE(KernelUtil::toJsonWrapper(wrapper, "no_exist", outWrapper));
        ASSERT_TRUE(KernelUtil::toJsonWrapper(wrapper, "hash_mode", outWrapper));
        string value;
        outWrapper.Jsonize("hash_function", value);
        ASSERT_EQ("HASH", value);
    }
    {
        Any any;
        FromJsonString(any, jsonStr);
        autil::legacy::Jsonizable::JsonWrapper wrapper(any);
        autil::legacy::Jsonizable::JsonWrapper outWrapper;
        ASSERT_FALSE(KernelUtil::toJsonWrapper(wrapper, "no_exist", outWrapper));
        ASSERT_TRUE(KernelUtil::toJsonWrapper(wrapper, "hash_mode", outWrapper));
        string value;
        outWrapper.Jsonize("hash_function", value);
        ASSERT_EQ("HASH", value);
    }

}


END_HA3_NAMESPACE(sql);

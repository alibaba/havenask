#include "matchdoc/toolkit/MatchDocHelper.h"

#include <memory>
#include <sstream>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <unistd.h>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;

namespace matchdoc {

class MatchDocHelperTest : public testing::Test {
public:
    void SetUp() { _pool.reset(new autil::mem_pool::Pool()); }

protected:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
};

TEST_F(MatchDocHelperTest, testPrepare) {
    {
        string schema = "";
        vector<string> data = {};
        vector<MatchDoc> docs;
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool));
        ASSERT_TRUE(MatchDocHelper::prepare(schema, data, allocator, docs));
        ASSERT_EQ(0, docs.size());
    }
    {
        string schema = "int:INT32;m_int:INT32:true;int64:INT64;m_int64:INT64:true;"
                        "dou:DOUBLE;m_dou:DOUBLE:true;str:STRING;m_str:STRING:true";
        vector<string> data = {"int=1,m_int=1 1,int64=1,m_int64=1 1,dou=1.1,m_dou=1.1 1.1,str=s1,m_str=s1 s1",
                               "int=2,m_int=2 2,int64=2,m_int64=2 2,dou=2.2,m_dou=2.2 2.2,str=s2,m_str=s2 s2"};
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool));
        vector<MatchDoc> docs;
        ASSERT_TRUE(MatchDocHelper::prepare(schema, data, allocator, docs));
        ASSERT_TRUE(MatchDocHelper::checkField<int32_t>(allocator, docs, "int", {1, 2}));
        ASSERT_TRUE(MatchDocHelper::checkMultiValField<int32_t>(allocator, docs, "m_int", {{1, 1}, {2, 2}}));
        ASSERT_TRUE(MatchDocHelper::checkField<int64_t>(allocator, docs, "int64", {1, 2}));
        ASSERT_TRUE(MatchDocHelper::checkMultiValField<int64_t>(allocator, docs, "m_int64", {{1, 1}, {2, 2}}));
        ASSERT_TRUE(MatchDocHelper::checkField<double>(allocator, docs, "dou", {1.1, 2.2}));
        ASSERT_TRUE(MatchDocHelper::checkMultiValField<double>(allocator, docs, "m_dou", {{1.1, 1.1}, {2.2, 2.2}}));
        ASSERT_TRUE(MatchDocHelper::checkField<string>(allocator, docs, "str", {"s1", "s2"}));
        ASSERT_TRUE(MatchDocHelper::checkMultiValField<string>(allocator, docs, "m_str", {{"s1", "s1"}, {"s2", "s2"}}));
    }
    {
        string schema = "int:int32;m_int:int32:true;int64:int64;m_int64:int64:true;"
                        "dou:double;m_dou:double:true;str:string;m_str:string:true";
        vector<string> data = {"int=1,m_int=1 1,int64=1,m_int64=1 1,dou=1.1,m_dou=1.1 1.1,str=s1,m_str=s1 s1",
                               "int=2,m_int=2 2,int64=2,m_int64=2 2,dou=2.2,m_dou=2.2 2.2,str=s2,m_str=s2 s2"};
        MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool));
        vector<MatchDoc> docs;
        ASSERT_TRUE(MatchDocHelper::prepare(schema, data, allocator, docs));
        ASSERT_TRUE(MatchDocHelper::checkField<int32_t>(allocator, docs, "int", {1, 2}));
        ASSERT_TRUE(MatchDocHelper::checkMultiValField<int32_t>(allocator, docs, "m_int", {{1, 1}, {2, 2}}));
        ASSERT_TRUE(MatchDocHelper::checkField<int64_t>(allocator, docs, "int64", {1, 2}));
        ASSERT_TRUE(MatchDocHelper::checkMultiValField<int64_t>(allocator, docs, "m_int64", {{1, 1}, {2, 2}}));
        ASSERT_TRUE(MatchDocHelper::checkField<double>(allocator, docs, "dou", {1.1, 2.2}));
        ASSERT_TRUE(MatchDocHelper::checkMultiValField<double>(allocator, docs, "m_dou", {{1.1, 1.1}, {2.2, 2.2}}));
        ASSERT_TRUE(MatchDocHelper::checkField<string>(allocator, docs, "str", {"s1", "s2"}));
        ASSERT_TRUE(MatchDocHelper::checkMultiValField<string>(allocator, docs, "m_str", {{"s1", "s1"}, {"s2", "s2"}}));
    }
}

TEST_F(MatchDocHelperTest, testPrepareFromJsonFile) {
    char cwdBuf[10240];
    ASSERT_TRUE(getcwd(cwdBuf, 10240) != nullptr);
    string curPath = cwdBuf;
    string schemaPath = curPath + "/aios/matchdoc/toolkit/test/testdata/schema.json";
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool));
    vector<MatchDoc> docs;
    ASSERT_TRUE(MatchDocHelper::prepareFromJsonFile(schemaPath, allocator, docs));
    ASSERT_TRUE(MatchDocHelper::checkField<int32_t>(allocator, docs, "int", {1, 2}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<int32_t>(allocator, docs, "m_int", {{1, 1}, {2, 2}}));
    ASSERT_TRUE(MatchDocHelper::checkField<int64_t>(allocator, docs, "int64", {1, 2}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<int64_t>(allocator, docs, "m_int64", {{1, 1}, {2, 2}}));
    ASSERT_TRUE(MatchDocHelper::checkField<double>(allocator, docs, "dou", {1.1, 2.2}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<double>(allocator, docs, "m_dou", {{1.1, 1.1}, {2.2, 2.2}}));
    ASSERT_TRUE(MatchDocHelper::checkField<string>(allocator, docs, "str", {"s1", "s2"}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<string>(allocator, docs, "m_str", {{"s1", "s1"}, {"s2", "s2"}}));
}

TEST_F(MatchDocHelperTest, testPrepareFromJson) {
    string schema = R"(
{
    "name": "schema0",
    "fields": [
        {
            "field_name": "int",
            "field_type": "INT32"
        },
        {
            "field_name": "m_int",
            "field_type": "INT32",
            "multi_value": true,
            "user_defined_param": {
                "seperator": ":"
            }
        },
        {
            "field_name": "int64",
            "field_type": "INT64"
        },
        {
            "field_name": "m_int64",
            "field_type": "INT64",
            "multi_value": true,
            "user_defined_param": {
                "seperator": ":"
            }
        },
        {
            "field_name": "dou",
            "field_type": "DOUBLE"
        },
        {
            "field_name": "m_dou",
            "field_type": "DOUBLE",
            "multi_value": true,
            "user_defined_param": {
                "seperator": ":"
            }
        },
        {
            "field_name": "str",
            "field_type": "STRING"
        },
        {
            "field_name": "m_str",
            "field_type": "STRING",
            "multi_value": true,
            "user_defined_param": {
                "seperator": ":"
            }
        }
    ],
    "field_seperator": ",",
    "docs": [
        "1, 1:1, 1, 1:1, 1.1, 1.1:1.1, s1, s1:s1 ",
        "2,2:2,2,2:2,2.2,2.2:2.2,s2,s2:s2"
    ]
}
    )";
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool));
    vector<MatchDoc> docs;
    ASSERT_TRUE(MatchDocHelper::prepareFromJson(schema, allocator, docs));
    ASSERT_TRUE(MatchDocHelper::checkField<int32_t>(allocator, docs, "int", {1, 2}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<int32_t>(allocator, docs, "m_int", {{1, 1}, {2, 2}}));
    ASSERT_TRUE(MatchDocHelper::checkField<int64_t>(allocator, docs, "int64", {1, 2}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<int64_t>(allocator, docs, "m_int64", {{1, 1}, {2, 2}}));
    ASSERT_TRUE(MatchDocHelper::checkField<double>(allocator, docs, "dou", {1.1, 2.2}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<double>(allocator, docs, "m_dou", {{1.1, 1.1}, {2.2, 2.2}}));
    ASSERT_TRUE(MatchDocHelper::checkField<string>(allocator, docs, "str", {"s1", "s2"}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<string>(allocator, docs, "m_str", {{"s1", "s1"}, {"s2", "s2"}}));
}

TEST_F(MatchDocHelperTest, testExtend) {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool));
    size_t docCount = 3;
    vector<MatchDoc> docs;
    for (size_t i = 0; i < docCount; ++i) {
        docs.push_back(allocator->allocate());
    }
    MatchDocHelper::extend<int32_t>(allocator, docs, "int_field", {11, 22, 33});
    MatchDocHelper::extend<float>(allocator, docs, "float_field", {1.1, 2.2, 3.3});
    MatchDocHelper::extend<double>(allocator, docs, "double_field", {11.1, 22.2, 33.3});
    ASSERT_FALSE(MatchDocHelper::extend<double>(allocator, docs, "double_field", {11.1, 22.2, 33.3, 44}));
    MatchDocHelper::extend<string>(allocator, docs, "str_field", {"aa", "bb", "cc"});
    MatchDocHelper::extendMultiVal<int32_t>(allocator, docs, "int_multi_field", {{1}, {2, 22}, {3, 33, 333}});
    MatchDocHelper::extendMultiVal<float>(
        allocator, docs, "float_multi_field", {{1.1}, {2.2, 22.2}, {3.3, 33.3, 333.3}});
    MatchDocHelper::extendMultiVal<double>(
        allocator, docs, "double_multi_field", {{0.1}, {0.2, 0.22}, {0.3, 0.33, 0.333}});
    MatchDocHelper::extendMultiVal<string>(
        allocator, docs, "str_multi_field", {{"a"}, {"b", "bb"}, {"c", "cc", "ccc"}});

    ASSERT_TRUE(MatchDocHelper::checkField<int32_t>(allocator, docs, "int_field", {11, 22, 33}));
    ASSERT_TRUE(MatchDocHelper::checkField<float>(allocator, docs, "float_field", {1.1, 2.2, 3.3}));
    ASSERT_TRUE(MatchDocHelper::checkField<double>(allocator, docs, "double_field", {11.1, 22.2, 33.3}));
    ASSERT_TRUE(MatchDocHelper::checkField<string>(allocator, docs, "str_field", {"aa", "bb", "cc"}));
    ASSERT_TRUE(
        MatchDocHelper::checkMultiValField<int32_t>(allocator, docs, "int_multi_field", {{1}, {2, 22}, {3, 33, 333}}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<float>(
        allocator, docs, "float_multi_field", {{1.1}, {2.2, 22.2}, {3.3, 33.3, 333.3}}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<double>(
        allocator, docs, "double_multi_field", {{0.1}, {0.2, 0.22}, {0.3, 0.33, 0.333}}));
    ASSERT_TRUE(MatchDocHelper::checkMultiValField<string>(
        allocator, docs, "str_multi_field", {{"a"}, {"b", "bb"}, {"c", "cc", "ccc"}}));
}

TEST_F(MatchDocHelperTest, testCheckField) {
    MatchDocAllocatorPtr leftAlloc(new MatchDocAllocator(_pool));
    MatchDocAllocatorPtr rightAlloc(new MatchDocAllocator(_pool));
    vector<MatchDoc> leftDocs;
    vector<MatchDoc> rightDocs;
    size_t docCount = 3;
    for (size_t i = 0; i < docCount; ++i) {
        leftDocs.push_back(leftAlloc->allocate());
        rightDocs.push_back(rightAlloc->allocate());
    }

    {
        MatchDocHelper::extend<int32_t>(leftAlloc, leftDocs, "int_field", {11, 22, 33});
        MatchDocHelper::extend<float>(leftAlloc, leftDocs, "float_field", {1.1, 2.2, 3.3});
        MatchDocHelper::extend<double>(leftAlloc, leftDocs, "double_field", {11.1, 22.2, 33.3});
        MatchDocHelper::extend<string>(leftAlloc, leftDocs, "str_field", {"aa", "bb", "cc"});
        MatchDocHelper::extendMultiVal<int32_t>(leftAlloc, leftDocs, "int_multi_field", {{1}, {2, 22}, {3, 33, 333}});
        MatchDocHelper::extendMultiVal<float>(
            leftAlloc, leftDocs, "float_multi_field", {{1.1}, {2.2, 22.2}, {3.3, 33.3, 333.3}});
        MatchDocHelper::extendMultiVal<double>(
            leftAlloc, leftDocs, "double_multi_field", {{0.1}, {0.2, 0.22}, {0.3, 0.33, 0.333}});
        MatchDocHelper::extendMultiVal<string>(
            leftAlloc, leftDocs, "str_multi_field", {{"a"}, {"b", "bb"}, {"c", "cc", "ccc"}});
    }

    {
        // diff type
        MatchDocHelper::extend<int8_t>(rightAlloc, rightDocs, "int_field", {11, 22, 33});
        // diff val
        MatchDocHelper::extend<float>(rightAlloc, rightDocs, "float_field", {1.1, 2.2, 3.31});
        MatchDocHelper::extend<double>(rightAlloc, rightDocs, "double_field", {11.1, 22.2, 33.3});
        MatchDocHelper::extend<string>(rightAlloc, rightDocs, "str_field", {"aa", "bb", "cc"});
        MatchDocHelper::extendMultiVal<int16_t>(rightAlloc, rightDocs, "int_multi_field", {{1}, {2, 22}, {3, 33, 333}});
        MatchDocHelper::extendMultiVal<float>(
            rightAlloc, rightDocs, "float_multi_field", {{1.1}, {2.2, 22.2}, {3.3, 33.3, 333.3}});
        MatchDocHelper::extendMultiVal<double>(
            rightAlloc, rightDocs, "double_multi_field", {{0.1}, {0.2, 0.22}, {0.3, 0.33, 0.333}});
        MatchDocHelper::extendMultiVal<string>(
            rightAlloc, rightDocs, "str_multi_field", {{"a"}, {"b", "bb"}, {"c", "cc", "ccc"}});
    }

    ASSERT_FALSE(MatchDocHelper::checkField(leftAlloc, leftDocs, rightAlloc, rightDocs, "int_field"));
    ASSERT_FALSE(MatchDocHelper::checkField(leftAlloc, leftDocs, rightAlloc, rightDocs, "float_field"));
    ASSERT_TRUE(MatchDocHelper::checkField(leftAlloc, leftDocs, rightAlloc, rightDocs, "double_field"));
    ASSERT_TRUE(MatchDocHelper::checkField(leftAlloc, leftDocs, rightAlloc, rightDocs, "str_field"));
    ASSERT_FALSE(MatchDocHelper::checkField(leftAlloc, leftDocs, rightAlloc, rightDocs, "int_multi_field"));
    ASSERT_TRUE(MatchDocHelper::checkField(leftAlloc, leftDocs, rightAlloc, rightDocs, "float_multi_field"));
    ASSERT_TRUE(MatchDocHelper::checkField(leftAlloc, leftDocs, rightAlloc, rightDocs, "double_multi_field"));
    ASSERT_TRUE(MatchDocHelper::checkField(leftAlloc, leftDocs, rightAlloc, rightDocs, "str_multi_field"));
    ASSERT_FALSE(MatchDocHelper::check(leftAlloc, leftDocs, rightAlloc, rightDocs));
    ASSERT_TRUE(MatchDocHelper::check(
        leftAlloc, leftDocs, rightAlloc, rightDocs, {"double_field", "str_field", "float_multi_field"}));
}

TEST_F(MatchDocHelperTest, testPrint) {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool));
    size_t docCount = 3;
    vector<MatchDoc> docs;
    for (size_t i = 0; i < docCount; ++i) {
        docs.push_back(allocator->allocate());
    }
    MatchDocHelper::extend<int32_t>(allocator, docs, "int_field", {11, 22, 33});
    MatchDocHelper::extend<float>(allocator, docs, "float_field", {1.1, 2.2, 3.3});
    MatchDocHelper::extend<double>(allocator, docs, "double_field", {11.1, 22.2, 33.3});
    ASSERT_FALSE(MatchDocHelper::extend<double>(allocator, docs, "double_field", {11.1, 22.2, 33.3, 44}));
    MatchDocHelper::extend<string>(allocator, docs, "str_field", {"aa", "bb", "cc"});
    MatchDocHelper::extendMultiVal<int32_t>(allocator, docs, "int_multi_field", {{1}, {2, 22}, {3, 33, 333}});
    MatchDocHelper::extendMultiVal<float>(
        allocator, docs, "float_multi_field", {{1.1}, {2.2, 22.2}, {3.3, 33.3, 333.3}});
    MatchDocHelper::extendMultiVal<double>(
        allocator, docs, "double_multi_field", {{0.1}, {0.2, 0.22}, {0.3, 0.33, 0.333}});
    MatchDocHelper::extendMultiVal<string>(
        allocator, docs, "str_multi_field", {{"a"}, {"b", "bb"}, {"c", "cc", "ccc"}});

    string expectVal = "double_field=11.1,double_multi_field=0.1,float_field=1.1,float_multi_field=1.1,int_field="
                       "11,int_multi_field=1,str_field=aa,str_multi_field=a\n"
                       "double_field=22.2,double_multi_field=0.2 0.22,float_field=2.2,float_multi_field=2.2 "
                       "22.2,int_field=22,int_multi_field=2 22,str_field=bb,str_multi_field=b bb\n"
                       "double_field=33.3,double_multi_field=0.3 0.33 0.333,float_field=3.3,float_multi_field=3.3 "
                       "33.3 333.3,int_field=33,int_multi_field=3 33 333,str_field=cc,str_multi_field=c cc ccc\n";
    ASSERT_EQ(expectVal,
              MatchDocHelper::print(allocator,
                                    docs,
                                    {"double_field",
                                     "double_multi_field",
                                     "float_field",
                                     "float_multi_field",
                                     "int_field",
                                     "int_multi_field",
                                     "str_field",
                                     "str_multi_field"}));

    string expectVal2 = "int_field=11,str_field=aa,str_multi_field=a\n"
                        "int_field=22,str_field=bb,str_multi_field=b bb\n"
                        "int_field=33,str_field=cc,str_multi_field=c cc ccc\n";
    ASSERT_EQ(expectVal2, MatchDocHelper::print(allocator, docs, {"int_field", "str_field", "str_multi_field"}));
}

TEST_F(MatchDocHelperTest, testPrintStdVector) {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool));
    constexpr size_t docCount = 3;
    vector<MatchDoc> docs;
    for (size_t i = 0; i < docCount; ++i) {
        docs.push_back(allocator->allocate());
    }
    vector<vector<int32_t>> intValues({{1}, {1, 2}, {1, 2, 3}});
    MatchDocHelper::extend<vector<int32_t>>(allocator, docs, "std_vector_int", intValues);
    vector<vector<float>> floatValues({{1}, {1, 2}, {1, 2, 3}});
    MatchDocHelper::extend<vector<float>>(allocator, docs, "std_vector_float", floatValues);
    vector<vector<string>> stringValues({{"a"}, {"a", "b"}, {"a", "b", "c"}});
    MatchDocHelper::extend<vector<string>>(allocator, docs, "std_vector_string", stringValues);
    std::string expectValue = "std_vector_int=1,std_vector_float=1,std_vector_string=a\n"
                              "std_vector_int=1 2,std_vector_float=1 2,std_vector_string=a b\n"
                              "std_vector_int=1 2 3,std_vector_float=1 2 3,std_vector_string=a b c\n";
    ASSERT_EQ(expectValue,
              MatchDocHelper::print(allocator, docs, {"std_vector_int", "std_vector_float", "std_vector_string"}));
}

TEST_F(MatchDocHelperTest, testCheckDoc) {
    MatchDocAllocatorPtr allocator(new MatchDocAllocator(_pool));
    size_t docCount = 3;
    vector<MatchDoc> docs;
    for (size_t i = 0; i < docCount; ++i) {
        docs.push_back(allocator->allocate());
    }
    MatchDocHelper::extend<int32_t>(allocator, docs, "int_field", {11, 22, 33});
    MatchDocHelper::extend<float>(allocator, docs, "float_field", {1.1, 2.2, 3.3});
    MatchDocHelper::extend<double>(allocator, docs, "double_field", {11.1, 22.2, 33.3});
    ASSERT_FALSE(MatchDocHelper::extend<double>(allocator, docs, "double_field", {11.1, 22.2, 33.3, 44}));
    MatchDocHelper::extend<string>(allocator, docs, "str_field", {"aa", "bb", "cc"});
    MatchDocHelper::extendMultiVal<int32_t>(allocator, docs, "int_multi_field", {{1}, {2, 22}, {3, 33, 333}});
    MatchDocHelper::extendMultiVal<float>(
        allocator, docs, "float_multi_field", {{1.1}, {2.2, 22.2}, {3.3, 33.3, 333.3}});
    MatchDocHelper::extendMultiVal<double>(
        allocator, docs, "double_multi_field", {{0.1}, {0.2, 0.22}, {0.3, 0.33, 0.333}});
    MatchDocHelper::extendMultiVal<string>(
        allocator, docs, "str_multi_field", {{"a"}, {"b", "bb"}, {"c", "cc", "ccc"}});

    ASSERT_TRUE(MatchDocHelper::checkDoc(
        "double_field=11.1,double_multi_field=0.1,float_field=1.1,float_multi_field=1.1,int_field="
        "11,int_multi_field=1,str_field=aa,str_multi_field=a",
        allocator,
        docs[0]));
    ASSERT_TRUE(
        MatchDocHelper::checkDoc("double_field=22.2,double_multi_field=0.2 0.22,float_field=2.2,float_multi_field=2.2 "
                                 "22.2,int_field=22,int_multi_field=2 22,str_field=bb,str_multi_field=b bb",
                                 allocator,
                                 docs[1]));
    ASSERT_TRUE(MatchDocHelper::checkDoc(
        "double_field=33.3,double_multi_field=0.3 0.33 0.333,float_field=3.3,float_multi_field=3.3 "
        "33.3 333.3,int_field=33,int_multi_field=3 33 333,str_field=cc,str_multi_field=c cc ccc",
        allocator,
        docs[2]));
}
} // namespace matchdoc

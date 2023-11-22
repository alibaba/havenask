#include "sql/resource/Ha3FunctionModelConverter.h"

#include "autil/legacy/legacy_jsonizable.h"
#include "fslib/util/FileUtil.h"
#include "iquan/common/catalog/FunctionModel.h"
#include "navi/log/NaviLogger.h"
#include "unittest/unittest.h"

using namespace std;
using namespace iquan;
using namespace autil::legacy;
using namespace testing;

namespace sql {
class Ha3FunctionModelConverterTest : public TESTBASE {
public:
    void setUp() override;
    void tearDown() override;

private:
    template <typename T>
    void loadFromFile(const std::string &fileName, T &json) {
        std::string testdataPath = GET_TEST_DATA_PATH() + "sql_resource/";
        std::string jsonFile = testdataPath + fileName;
        std::string jsonStr;
        ASSERT_TRUE(fslib::util::FileUtil::readFile(jsonFile, jsonStr));
        try {
            autil::legacy::FastFromJsonString(json, jsonStr);
        } catch (const std::exception &e) { ASSERT_TRUE(false) << e.what(); }
    }
};
AUTIL_DECLARE_AND_SETUP_LOGGER(resource, Ha3FunctionModelConverterTest);

void Ha3FunctionModelConverterTest::setUp() {}

void Ha3FunctionModelConverterTest::tearDown() {}

TEST_F(Ha3FunctionModelConverterTest, testConverFunctionModelGeneral) {
    FunctionModel udf1;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udf1_with_acc.json", udf1));
    // convert success
    {
        FunctionModel udf1test = udf1;
        EXPECT_EQ(Status::OK(), Ha3FunctionModelConverter::convert(udf1test));
        string expectStr
            = R"json({"function_name":"udf1","function_type":"UDF","function_version":1,"is_deterministic":1,"function_content_version":"json_default_0.1","function_content":{"prototypes":[{"returns":[{"type":"string"}],"params":[{"type":"array","value_type":{"type":"string"}}],"variable_args":false},{"returns":[{"type":"int32"}],"params":[{"type":"array","value_type":{"type":"int32"}}],"variable_args":false,"acc_types":[{"type":"array","value_type":{"type":"double"}}]},{"returns":[{"type":"int64"}],"params":[{"type":"array","value_type":{"type":"int64"}}],"variable_args":false}],"properties":{}}})json";
        EXPECT_EQ(expectStr, FastToJsonString(udf1test));
    }
    // return type has empty type
    {
        FunctionModel udf1test = udf1;
        udf1test.functionDef.prototypes[0].returnTypes[0].isMulti = false;
        udf1test.functionDef.prototypes[0].returnTypes[0].type = "";
        EXPECT_NE(Status::OK(), Ha3FunctionModelConverter::convert(udf1test));
    }
    // param type has empty type
    {
        FunctionModel udf1test = udf1;
        udf1test.functionDef.prototypes[0].paramTypes[0].isMulti = false;
        udf1test.functionDef.prototypes[0].paramTypes[0].type = "";
        EXPECT_NE(Status::OK(), Ha3FunctionModelConverter::convert(udf1test));
    }
    // acc type has empty type
    {
        FunctionModel udf1test = udf1;
        udf1test.functionDef.prototypes[1].accTypes[0].isMulti = false;
        udf1test.functionDef.prototypes[1].accTypes[0].type = "";
        EXPECT_NE(Status::OK(), Ha3FunctionModelConverter::convert(udf1test));
    }
}

TEST_F(Ha3FunctionModelConverterTest, testConvertFunctionModelArrayType) {
    FunctionModel udf2;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udf2.json", udf2));
    // convert success
    EXPECT_EQ(Status::OK(), Ha3FunctionModelConverter::convert(udf2));
    string expectStr
        = R"json({"function_name":"matchscore2","function_type":"UDF","function_version":1,"is_deterministic":1,"function_content_version":"json_default_0.1","function_content":{"prototypes":[{"returns":[{"type":"double"}],"params":[{"type":"array","value_type":{"type":"int32"}},{"type":"string"},{"type":"double"}],"variable_args":false},{"returns":[{"type":"double"}],"params":[{"type":"array","value_type":{"type":"int64"}},{"type":"string"},{"type":"double"}],"variable_args":false}],"properties":{}}})json";
    ASSERT_EQ(expectStr, FastToJsonString(udf2));
}

TEST_F(Ha3FunctionModelConverterTest, testConvertFunctionModels) {
    FunctionModel f1;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udf1_with_acc.json", f1));
    FunctionModel f2;
    ASSERT_NO_FATAL_FAILURE(loadFromFile("udf2.json", f2));
    vector<FunctionModel> functions = {f1, f2};
    // convert success
    {
        auto functions1 = functions;
        EXPECT_EQ(Status::OK(), Ha3FunctionModelConverter::convert(functions1));
    }
    // first function convert failed
    {
        auto functions2 = functions;
        functions2[0].functionDef.prototypes[0].returnTypes[0].type = "";
        functions2[0].functionDef.prototypes[0].returnTypes[0].isMulti = false;
        EXPECT_NE(Status::OK(), Ha3FunctionModelConverter::convert(functions2));
    }
    {
        auto functions3 = functions;
        functions3[1].functionDef.prototypes[0].returnTypes[0].type = "";
        functions3[1].functionDef.prototypes[0].returnTypes[0].isMulti = false;
        EXPECT_NE(Status::OK(), Ha3FunctionModelConverter::convert(functions3));
    }
}

} // namespace sql

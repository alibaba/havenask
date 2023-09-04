#include "indexlib/common/field_format/customized_index/test/customized_index_field_encoder_unittest.h"

#include "indexlib/test/schema_maker.h"

using namespace std;

using namespace indexlib::test;
using namespace indexlib::config;

namespace indexlib { namespace common {
IE_LOG_SETUP(date, CustomizedIndexFieldEncoderTest);

CustomizedIndexFieldEncoderTest::CustomizedIndexFieldEncoderTest() {}

CustomizedIndexFieldEncoderTest::~CustomizedIndexFieldEncoderTest() {}

void CustomizedIndexFieldEncoderTest::CaseSetUp() {}

void CustomizedIndexFieldEncoderTest::CaseTearDown() {}

void CustomizedIndexFieldEncoderTest::TestSimpleProcess()
{
    string index = "embedding_index:customized:embedding;";
    string attribute = "embedding";

    IndexPartitionSchemaPtr floatSchema = SchemaMaker::MakeSchema("embedding:float", index, attribute, "");
    CustomizedIndexFieldEncoder encoder(floatSchema);
    {
        vector<dictkey_t> dictKeys;
        encoder.Encode(0, "1.23456", dictKeys);
        ASSERT_EQ(1, dictKeys.size());

        float result = reinterpret_cast<float&>(dictKeys[0]);
        EXPECT_FLOAT_EQ(1.23456, result);
    }

    {
        vector<dictkey_t> dictKeys;
        encoder.Encode(1, "1.23456", dictKeys);
        ASSERT_EQ(0, dictKeys.size());
    }

    {
        vector<dictkey_t> dictKeys;
        encoder.Encode(0, "", dictKeys);
        ASSERT_EQ(0, dictKeys.size());
    }

    {
        vector<dictkey_t> dictKeys;
        encoder.Encode(0, "abc", dictKeys);
        ASSERT_EQ(0, dictKeys.size());
    }
}

void CustomizedIndexFieldEncoderTest::TestEncodeDouble()
{
    string index = "embedding_index:customized:embedding;";
    string attribute = "embedding";

    IndexPartitionSchemaPtr floatSchema = SchemaMaker::MakeSchema("embedding:double", index, attribute, "");
    CustomizedIndexFieldEncoder encoder(floatSchema);
    vector<dictkey_t> dictKeys;
    encoder.Encode(0, "1.2345678987", dictKeys);
    ASSERT_EQ(1, dictKeys.size());

    double result = reinterpret_cast<double&>(dictKeys[0]);
    EXPECT_DOUBLE_EQ(1.2345678987, result);
}

}} // namespace indexlib::common

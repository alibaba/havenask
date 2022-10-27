#include "indexlib/util/test/float_int8_encoder_unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, FloatInt8EncoderTest);

FloatInt8EncoderTest::FloatInt8EncoderTest()
{
}

FloatInt8EncoderTest::~FloatInt8EncoderTest()
{
}

void FloatInt8EncoderTest::CaseSetUp()
{
}

void FloatInt8EncoderTest::CaseTearDown()
{
}

void FloatInt8EncoderTest::TestSimpleProcess()
{
    float absMax = 1.0;
    int32_t inputCount = 1024;
    float input[inputCount];
    for (int32_t i = 0; i < inputCount; i++) {
        input[i] = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
    }
    
    Pool pool;
    ConstString encodedData = FloatInt8Encoder::Encode(
            absMax, input, inputCount, &pool);

    float output[inputCount];
    int32_t decodedLen = FloatInt8Encoder::Decode(absMax, encodedData,
            (char*)output, sizeof(float) * inputCount);

    ASSERT_EQ(inputCount, decodedLen);
    for (int32_t i = 0; i < inputCount; i++) {
        ASSERT_TRUE(fabs(input[i] - output[i]) <= 0.01) << input[i] << "," << output[i];
    }
}

void FloatInt8EncoderTest::TestEncodeSingleValue()
{
    float absMax = 10;
    float input = 9.1;

    char output[1];
    FloatInt8Encoder::Encode(absMax, input, output);

    ConstString encodedData(output, 1);
    float decodeValue = 0.0;
    int32_t count = FloatInt8Encoder::Decode(absMax, encodedData, (char*)&decodeValue);
    ASSERT_EQ(1, count);
    ASSERT_TRUE(abs(input - decodeValue) <= 0.1) << input << "," << decodeValue;
}

void FloatInt8EncoderTest::TestCompare()
{
    float absMax = 1.0;
    int32_t inputCount = 1024;
    vector<float> input;
    input.resize(inputCount);
    for (int32_t i = 0; i < inputCount; i++) {
        input[i] = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
    }
    sort(input.begin(), input.end());
    
    int8_t output[inputCount];
    FloatInt8Encoder::Encode(absMax, input[0], (char*)&output[0]);
    for (int32_t i = 1; i < inputCount; i++) {
        FloatInt8Encoder::Encode(absMax, input[i], (char*)&output[i]);
        ASSERT_GE(output[i], output[i-1]);
    }
}

void FloatInt8EncoderTest::TestEncodeZero()
{
    int8_t testValue;
    FloatInt8Encoder::Encode(1.0, 0.0, (char*)&testValue);
    ASSERT_EQ(0, testValue);
    for (size_t i = 0; i < 1000; i++) {
        float absMax = static_cast<float>(rand());
        FloatInt8Encoder::Encode(absMax, 0.0, (char*)&testValue);
        ASSERT_EQ(0, testValue);
    }
}

IE_NAMESPACE_END(util);


#include "indexlib/util/test/fp16_encoder_unittest.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, Fp16EncoderTest);

Fp16EncoderTest::Fp16EncoderTest()
{
}

Fp16EncoderTest::~Fp16EncoderTest()
{
}

void Fp16EncoderTest::CaseSetUp()
{
}

void Fp16EncoderTest::CaseTearDown()
{
}

void Fp16EncoderTest::TestSimpleProcess()
{
    int32_t inputCount = 1024;
    float input[inputCount];
    for (int32_t i = 0; i < inputCount; i++) {
        input[i] = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
    }
    Pool pool;
    ConstString encodedData = Fp16Encoder::Encode(input, inputCount, &pool);

    float output[inputCount];
    int32_t decodedLen = Fp16Encoder::Decode(
        encodedData, (char*)output, sizeof(float) * inputCount);

    ASSERT_EQ(inputCount, decodedLen);
    for (int32_t i = 0; i < inputCount; i++) {
        ASSERT_TRUE(abs(input[i] - output[i]) <= 0.01) << input[i] << "," << output[i];
    }
}

void Fp16EncoderTest::TestEncodeSingleValue()
{
    char buffer[2];
    float value = 1.2;
    Fp16Encoder::Encode(value, buffer);

    ConstString encodedData(buffer, 2);
    float output = 0.0;
    int32_t count = Fp16Encoder::Decode(encodedData, (char*)&output);
    ASSERT_EQ(1, count);
    ASSERT_TRUE(abs(value - output) <= 0.001) << value << "," << output;
}

void Fp16EncoderTest::TestEncodeZero()
{
    int16_t testValue;
    Fp16Encoder::Encode(0.0, (char*)&testValue);
    ASSERT_EQ(0, testValue);
}

void Fp16EncoderTest::TestCompare()
{
    size_t count = 1024;
    vector<int16_t> outputs;
    vector<float> randomFloats;
    for (size_t i = 0; i < count; i++) {
        float tmp = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
        randomFloats.push_back(tmp);
    }
    sort(randomFloats.begin(), randomFloats.end());
    for (size_t i = 0; i < count; i++) {
        int16_t buffer;
        Fp16Encoder::Encode(randomFloats[i], (char*)&buffer);
        outputs.push_back(buffer);
    }
    for (size_t i = 1; i < count; i++) {
        ASSERT_GE(outputs[i], outputs[i-1]);
    }
}

IE_NAMESPACE_END(util);


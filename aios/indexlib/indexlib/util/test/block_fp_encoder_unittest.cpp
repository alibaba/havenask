#include "indexlib/util/test/block_fp_encoder_unittest.h"
#include "indexlib/storage/file_system_wrapper.h"
#include <cmath>

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(storage);

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BlockFpEncoderTest);

BlockFpEncoderTest::BlockFpEncoderTest()
{
}

BlockFpEncoderTest::~BlockFpEncoderTest()
{
}

void BlockFpEncoderTest::CaseSetUp()
{
}

void BlockFpEncoderTest::CaseTearDown()
{
}

void BlockFpEncoderTest::TestSimpleProcess()
{
    int32_t inputCount = 3;
    float input[] = {0.04, -0.0034, 0.025};
    Pool pool;
    ConstString encodedData = BlockFpEncoder::Encode(input, inputCount, &pool);

    float output[inputCount];
    int32_t decodedLen = BlockFpEncoder::Decode(
        encodedData, (char*)output, sizeof(float) * inputCount);

    ASSERT_EQ(inputCount, decodedLen);
    for (int32_t i = 0; i < inputCount; i++) {
        ASSERT_TRUE(abs(input[i] - output[i]) <= 0.00001)
            << input[i] << "," << output[i];
    }
}

void BlockFpEncoderTest::TestCompressFromFile()
{
    string inputFile = FileSystemWrapper::JoinPath(
        TEST_DATA_PATH, "/compress/float_compress/d0_0x100f81000_2318688.txt");
    string expFile = FileSystemWrapper::JoinPath(
        TEST_DATA_PATH, "/compress/float_compress/d0_0x100f81000_2318688_exp8.txt");
    string refFile = FileSystemWrapper::JoinPath(
        TEST_DATA_PATH, "/compress/float_compress/d0_0x100f81000_2318688_int16.txt");

    string content;
    FileSystemWrapper::AtomicLoad(inputFile, content);
    string expStr;
    FileSystemWrapper::AtomicLoad(expFile, expStr);
    string refStr;
    FileSystemWrapper::AtomicLoad(refFile, refStr);
    
    int32_t inputCount = content.size() / sizeof(float);
    inputCount = inputCount / 32 * 32;

    Pool pool;
    float* inputFloat = (float*)content.c_str();
    for (int32_t i = 0; i < inputCount; i += 32)
    {
        ConstString encodedData = BlockFpEncoder::Encode(inputFloat + i, 32, &pool);
        ASSERT_EQ(BlockFpEncoder::GetEncodeBytesLen(32), encodedData.size());

        float output[32];
        int32_t decodedLen = BlockFpEncoder::Decode(encodedData, (char*)output, sizeof(float) * 32);
        ASSERT_EQ(32, decodedLen);
        for (int32_t j = 0; j < 32; j++) {
            ASSERT_TRUE(abs(inputFloat[i + j] - output[j]) <= 0.00001)
                << inputFloat[i + j] << "," << output[j];
        }

        char* cursor = (char*)encodedData.data();
        // check exp
        uint8_t exp = ((uint8_t*)expStr.data())[i / 32];
        ASSERT_EQ(exp, *(uint8_t*)cursor);
        cursor++;
        
        // check int16
        uint16_t* refCursor = (uint16_t*)refStr.data();
        for (int32_t j = 0; j < 32; j++) {
            ASSERT_EQ(refCursor[i + j], *(uint16_t*)cursor);
            cursor += sizeof(uint16_t);
        }
    }
}

IE_NAMESPACE_END(util);


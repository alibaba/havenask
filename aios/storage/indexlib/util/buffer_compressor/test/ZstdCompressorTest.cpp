#include "indexlib/util/buffer_compressor/ZstdCompressor.h"

#include <zdict.h>

#include "BufferCompressorTest.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {

class ZstdCompressorTest : public BufferCompressorTest
{
public:
    ZstdCompressorTest();
    ~ZstdCompressorTest();

    DECLARE_CLASS_NAME(ZstdCompressorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForTrainHintData();
    void TestSetCompressLevel();

private:
    BufferCompressorPtr CreateCompressor() const override;
};

INDEXLIB_UNIT_TEST_CASE(ZstdCompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(ZstdCompressorTest, TestCaseForCompressLargeData);
INDEXLIB_UNIT_TEST_CASE(ZstdCompressorTest, TestCaseForTrainHintData);
INDEXLIB_UNIT_TEST_CASE(ZstdCompressorTest, TestSetCompressLevel);

using namespace std;

ZstdCompressorTest::ZstdCompressorTest() {}

ZstdCompressorTest::~ZstdCompressorTest() {}

void ZstdCompressorTest::CaseSetUp() {}

void ZstdCompressorTest::CaseTearDown() {}

BufferCompressorPtr ZstdCompressorTest::CreateCompressor() const { return BufferCompressorPtr(new ZstdCompressor); }

void ZstdCompressorTest::TestSetCompressLevel()
{
    ZstdCompressor compressor;
    ASSERT_EQ(1, compressor._compressLevel);

    {
        util::KeyValueMap param = {{"compress_level", "4"}};
        ZstdCompressor compressor(param);
        ASSERT_EQ(4, compressor._compressLevel);
    }

    {
        util::KeyValueMap param = {{"compress_level", "30"}};
        ZstdCompressor compressor(param);
        ASSERT_EQ(1, compressor._compressLevel);
    }
}

void ZstdCompressorTest::TestCaseForTrainHintData()
{
    size_t strLen = 17 * 1024 * 1024;
    string org;
    org.reserve(strLen);
    for (size_t i = 0; i < strLen; i += 32) {
        org.append(32, (char)((i * 13) % 128));
    }

    size_t maxBlockSize = 4 * 1024;
    size_t trainBlockCount = 1024;
    float sampleRatio = 0.1f;
    string result;

    vector<string> hintDataHoldVec;
    util::BufferCompressorPtr compressor = CreateCompressor();
    CompressHintDataTrainerPtr trainer(compressor->CreateTrainer(maxBlockSize, trainBlockCount, sampleRatio));
    auto DoTrainAndCompressTest = [&](const CompressHintDataTrainerPtr& trainer,
                                      const util::BufferCompressorPtr& compressor, bool needCopy) {
        ASSERT_TRUE(trainer->TrainHintData());
        auto hintData = trainer->GetHintData();
        if (!needCopy) {
            hintDataHoldVec.push_back(std::string(hintData.data(), hintData.size()));
            hintData = autil::StringView(*hintDataHoldVec.rbegin());
        }
        CompressHintDataPtr hintDataObj(compressor->CreateHintData(hintData, needCopy));
        for (size_t j = 0; j < trainer->GetCurrentBlockCount(); j++) {
            autil::StringView data = trainer->GetBlockData(j);
            compressor->AddDataToBufferIn(data.data(), data.size());
            ASSERT_TRUE(compressor->Compress(hintData));
            string compressedStr = string(compressor->GetBufferOut(), compressor->GetBufferOutLen());
            compressor->Reset();

            compressor->AddDataToBufferIn(compressedStr);
            ASSERT_TRUE(compressor->Decompress(hintDataObj.get(), maxBlockSize)) << j;
            string decompressedStr = string(compressor->GetBufferOut(), compressor->GetBufferOutLen());
            compressor->Reset();
            result += decompressedStr;
            ASSERT_TRUE(data == decompressedStr);
        }
        trainer->Reset();
    };

    size_t trainCount = 0;
    for (size_t i = 0; i < strLen; i += maxBlockSize) {
        size_t len = (i + maxBlockSize) > strLen ? strLen - i : maxBlockSize;
        trainer->AddOneBlockData(org.data() + i, len);
        if (trainer->GetCurrentBlockCount() == trainBlockCount) {
            DoTrainAndCompressTest(trainer, compressor, (trainCount % 2 == 0));
            trainCount++;
        }
    }
    if (trainer->GetCurrentBlockCount() > 0) {
        DoTrainAndCompressTest(trainer, compressor, false);
    }
    ASSERT_EQ(result, org);
}

}} // namespace indexlib::util

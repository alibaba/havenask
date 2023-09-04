#include "indexlib/util/buffer_compressor/Lz4Compressor.h"

#include "BufferCompressorTest.h"
#include "indexlib/util/testutil/unittest.h"

namespace indexlib { namespace util {

class Lz4CompressorTest : public BufferCompressorTest
{
public:
    Lz4CompressorTest();
    ~Lz4CompressorTest();

    DECLARE_CLASS_NAME(Lz4CompressorTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestCaseForTrainHintData();

private:
    BufferCompressorPtr CreateCompressor() const override;
};

INDEXLIB_UNIT_TEST_CASE(Lz4CompressorTest, TestCaseForCompress);
INDEXLIB_UNIT_TEST_CASE(Lz4CompressorTest, TestCaseForCompressLargeData);
INDEXLIB_UNIT_TEST_CASE(Lz4CompressorTest, TestCaseForTrainHintData);

using namespace std;

Lz4CompressorTest::Lz4CompressorTest() : BufferCompressorTest(false) {}

Lz4CompressorTest::~Lz4CompressorTest() {}

void Lz4CompressorTest::CaseSetUp() {}

void Lz4CompressorTest::CaseTearDown() {}

BufferCompressorPtr Lz4CompressorTest::CreateCompressor() const { return BufferCompressorPtr(new Lz4Compressor); }

void Lz4CompressorTest::TestCaseForTrainHintData()
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

    util::BufferCompressorPtr compressor = CreateCompressor();
    CompressHintDataTrainerPtr trainer(compressor->CreateTrainer(maxBlockSize, trainBlockCount, sampleRatio));
    auto DoTrainAndCompressTest = [&](const CompressHintDataTrainerPtr& trainer,
                                      const util::BufferCompressorPtr& compressor) {
        ASSERT_TRUE(trainer->TrainHintData());
        CompressHintDataPtr hintData(compressor->CreateHintData(trainer->GetHintData(), true));
        for (size_t j = 0; j < trainer->GetCurrentBlockCount(); j++) {
            autil::StringView data = trainer->GetBlockData(j);
            compressor->AddDataToBufferIn(data.data(), data.size());
            ASSERT_TRUE(compressor->Compress(trainer->GetHintData()));
            string compressedStr = string(compressor->GetBufferOut(), compressor->GetBufferOutLen());
            compressor->Reset();

            compressor->AddDataToBufferIn(compressedStr);
            ASSERT_TRUE(compressor->Decompress(hintData.get(), maxBlockSize)) << j;
            string decompressedStr = string(compressor->GetBufferOut(), compressor->GetBufferOutLen());
            compressor->Reset();
            result += decompressedStr;
            ASSERT_TRUE(data == decompressedStr);
        }
        trainer->Reset();
    };

    for (size_t i = 0; i < strLen; i += maxBlockSize) {
        size_t len = (i + maxBlockSize) > strLen ? strLen - i : maxBlockSize;
        trainer->AddOneBlockData(org.data() + i, len);
        if (trainer->GetCurrentBlockCount() == trainBlockCount) {
            DoTrainAndCompressTest(trainer, compressor);
        }
    }
    if (trainer->GetCurrentBlockCount() > 0) {
        DoTrainAndCompressTest(trainer, compressor);
    }
    ASSERT_EQ(result, org);
}
}} // namespace indexlib::util

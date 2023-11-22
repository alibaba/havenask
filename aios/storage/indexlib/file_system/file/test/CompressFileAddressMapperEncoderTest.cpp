#include "indexlib/file_system/file/CompressFileAddressMapperEncoder.h"

#include "indexlib/util/testutil/unittest.h"

using namespace std;

namespace indexlib { namespace file_system {

class CompressFileAddressMapperEncoderTest : public INDEXLIB_TESTBASE
{
public:
    CompressFileAddressMapperEncoderTest();
    ~CompressFileAddressMapperEncoderTest();

    DECLARE_CLASS_NAME(CompressFileAddressMapperEncoderTest);

public:
    void CaseSetUp() override;
    void CaseTearDown() override;
    void TestSimpleProcess();

private:
    AUTIL_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(CompressFileAddressMapperEncoderTest, TestSimpleProcess);
AUTIL_LOG_SETUP(indexlib.file_system, CompressFileAddressMapperEncoderTest);

CompressFileAddressMapperEncoderTest::CompressFileAddressMapperEncoderTest() {}

CompressFileAddressMapperEncoderTest::~CompressFileAddressMapperEncoderTest() {}

void CompressFileAddressMapperEncoderTest::CaseSetUp() {}

void CompressFileAddressMapperEncoderTest::CaseTearDown() {}

void CompressFileAddressMapperEncoderTest::TestSimpleProcess()
{
    auto CheckEncode = [](const vector<size_t>& offsets, bool expectReturn, size_t maxBatchNum) {
        vector<char> encodeData;
        CompressFileAddressMapperEncoder encoder(maxBatchNum);
        encodeData.resize(encoder.CalculateEncodeSize(offsets.size()));
        ASSERT_EQ(expectReturn,
                  encoder.Encode((size_t*)offsets.data(), offsets.size(), (char*)encodeData.data(), encodeData.size()));
        if (expectReturn) {
            for (size_t i = 0; i < offsets.size(); i++) {
                ASSERT_EQ(offsets[i], encoder.GetOffset((char*)encodeData.data(), i));
            }
        }
    };

    {
        vector<size_t> offsets = {0, 18, 24, 36, 47, 52};
        CheckEncode(offsets, true, 16);
        CheckEncode(offsets, true, 8);
        CheckEncode(offsets, true, 10);
    }

    {
        vector<size_t> offsets;
        for (size_t i = 0; i < 16; i++) {
            offsets.push_back(i * 64 + i);
        }
        CheckEncode(offsets, true, 0);
        CheckEncode(offsets, true, 16);
        CheckEncode(offsets, true, 8);
        CheckEncode(offsets, true, 10);
    }

    {
        vector<size_t> offsets;
        for (size_t i = 0; i < 162; i++) {
            offsets.push_back(i * 64 + i);
        }

        CheckEncode(offsets, true, 0);
        CheckEncode(offsets, true, 16);
        CheckEncode(offsets, true, 8);
        CheckEncode(offsets, true, 10);
    }

    {
        // not increase value
        vector<size_t> offsets = {2126, 21, 1243};
        CheckEncode(offsets, false, 16);
        CheckEncode(offsets, false, 8);
        CheckEncode(offsets, false, 10);
    }

    {
        // delta over max value of uint16_t
        vector<size_t> offsets = {1, 2126, 1232346};
        CheckEncode(offsets, false, 16);
        CheckEncode(offsets, false, 8);
        CheckEncode(offsets, false, 10);
    }
}

}} // namespace indexlib::file_system

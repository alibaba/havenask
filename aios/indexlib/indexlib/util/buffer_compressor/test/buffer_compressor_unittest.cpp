#include "indexlib/util/buffer_compressor/test/buffer_compressor_unittest.h"

using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, BufferCompressorTest);

BufferCompressorTest::BufferCompressorTest(bool supportStream)
    : mSupportStream(supportStream)
{
}

BufferCompressorTest::~BufferCompressorTest()
{
}

void BufferCompressorTest::CaseSetUp()
{
}

void BufferCompressorTest::CaseTearDown()
{
}

void BufferCompressorTest::TestCaseForCompress()
{
    util::BufferCompressorPtr compressor = CreateCompressor();
    compressor->SetBufferInLen(256);
    compressor->SetBufferOutLen(256);
    int strLen = 2560;

    string org = "";
    for(int i = 0; i < strLen; i += 2)
    {
        compressor->Reset();
        org.append(1, (char)(i * i));
        compressor->AddDataToBufferIn(org.c_str(), org.length());
            
        string added = "xyz124";
        for(int j = 0; j < i % 5; j++)
        {
            added.append(1, (char)(j * 3));
        }
        compressor->AddDataToBufferIn(added.c_str(), added.length());
        org.append(added);

        bool ret = compressor->Compress();
        ASSERT_TRUE(ret);
        uint32_t compressedLen = compressor->GetBufferOutLen();
        string compressedStr = string(compressor->GetBufferOut(),compressedLen);
       
        compressor->Reset();
        compressor->AddDataToBufferIn(compressedStr.c_str(), compressedStr.length());
        ret = compressor->Decompress(org.size());
        ASSERT_TRUE(ret);
        string decompressedStr = string(compressor->GetBufferOut(), compressor->GetBufferOutLen());
        ASSERT_EQ(decompressedStr , org);
        if (mSupportStream)
        {
            compressor->Reset();
            compressor->GetOutBuffer().release();
            compressor->AddDataToBufferIn(compressedStr.c_str(), compressedStr.length());
            ret = compressor->Decompress();
            ASSERT_TRUE(ret);
            string decompressedStr = string(compressor->GetBufferOut(), compressor->GetBufferOutLen());
            ASSERT_EQ(decompressedStr, org);
        }
    }
}

void BufferCompressorTest::TestCaseForCompressLargeData()
{
    size_t strLen = 2 * 1024 * 1024; //2M

    string org;
    org.reserve(strLen);
    for (size_t i = 0; i < strLen; i += 32) {
        org.append(32, (char)((i * 13) % 128));
    }

    util::BufferCompressorPtr compressor = CreateCompressor();
    compressor->AddDataToBufferIn(org);
    bool ret = compressor->Compress();
    ASSERT_TRUE(ret);
        
    uint32_t compressedLen = compressor->GetBufferOutLen();
    string compressedStr = string(compressor->GetBufferOut(), compressedLen);
       
    compressor->Reset();
    compressor->AddDataToBufferIn(compressedStr);

    ret = compressor->Decompress(org.size());
    ASSERT_TRUE(ret);
        
    string decompressedStr = string(compressor->GetBufferOut(), compressor->GetBufferOutLen());
    ASSERT_EQ(decompressedStr , org);

    if (mSupportStream)
    {
        compressor->Reset();
        compressor->GetOutBuffer().release();
        compressor->AddDataToBufferIn(compressedStr);

        ret = compressor->Decompress();
        ASSERT_TRUE(ret);
        
        string decompressedStr = string(compressor->GetBufferOut(), compressor->GetBufferOutLen());
        ASSERT_EQ(decompressedStr , org);        
    }
}

IE_NAMESPACE_END(util);


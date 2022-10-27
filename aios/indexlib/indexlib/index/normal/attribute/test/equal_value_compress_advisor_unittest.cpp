#include "indexlib/index/normal/attribute/test/equal_value_compress_advisor_unittest.h"

using namespace std;
IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, EqualValueCompressAdvisorTest);

template <typename T>
class MockEquivalentCompressWriter
{
public:
    MockEquivalentCompressWriter() {}
    ~MockEquivalentCompressWriter() {}

public:
    static void Init(size_t *array, size_t count);
    static size_t CalculateCompressLength(
        const EquivalentCompressReader<T>& reader,
        uint32_t slotItemCount);

    static uint32_t GetIdx();
private:
    static uint32_t ARRAY_IDX;
    static size_t   SIZE_ARRAY[5];
};

template <typename T>
uint32_t MockEquivalentCompressWriter<T>::ARRAY_IDX = 0;

template <typename T>
size_t MockEquivalentCompressWriter<T>::SIZE_ARRAY[5] = {0};

template <typename T>
void MockEquivalentCompressWriter<T>::Init(size_t *array, size_t count)
{
    ARRAY_IDX = 0;
    for(size_t i = 0; i < count; ++i)
    {
        SIZE_ARRAY[i] = array[i];
    }
}

template <typename T>
uint32_t MockEquivalentCompressWriter<T>::GetIdx()
{
    return ARRAY_IDX;
}

template <typename T>
size_t MockEquivalentCompressWriter<T>::CalculateCompressLength(
        const EquivalentCompressReader<T>& reader,
        uint32_t slotItemCount)
{
    return SIZE_ARRAY[ARRAY_IDX++];
}

EqualValueCompressAdvisorTest::EqualValueCompressAdvisorTest()
{
}

EqualValueCompressAdvisorTest::~EqualValueCompressAdvisorTest()
{
}

void EqualValueCompressAdvisorTest::CaseSetUp()
{
}

void EqualValueCompressAdvisorTest::CaseTearDown()
{
}

void EqualValueCompressAdvisorTest::TestGetOptSlotItemCount()
{
    {
        size_t array[] = {3/* opt */,  6,  7, 8, 9};

        MockEquivalentCompressWriter<uint32_t>::Init(array, 5);

        uint32_t buffer[] = {2, 2};
        EquivalentCompressReader<uint32_t> reader((uint8_t*)buffer);

        size_t optCompLen;
        uint32_t itemCount = 
            EqualValueCompressAdvisor<uint32_t, MockEquivalentCompressWriter<uint32_t> >::GetOptSlotItemCount(reader, optCompLen);

        ASSERT_EQ((uint32_t)(1<<6), itemCount);
        ASSERT_EQ((size_t)3, optCompLen);

        uint32_t idx = MockEquivalentCompressWriter<uint32_t>::GetIdx();
        ASSERT_EQ((uint32_t)2, idx);
    }

    {
        size_t array[] = {9, 3/* opt */,  6,  7, 8};

        MockEquivalentCompressWriter<uint32_t>::Init(array, 5);

        uint32_t buffer[] = {2, 2};
        EquivalentCompressReader<uint32_t> reader((uint8_t*)buffer);

        size_t optCompLen;
        uint32_t itemCount = 
            EqualValueCompressAdvisor<uint32_t, MockEquivalentCompressWriter<uint32_t> >::GetOptSlotItemCount(reader, optCompLen);

        ASSERT_EQ((uint32_t)(1<<7), itemCount);
        ASSERT_EQ((size_t)3, optCompLen);

        uint32_t idx = MockEquivalentCompressWriter<uint32_t>::GetIdx();
        ASSERT_EQ((uint32_t)3, idx);
    }

    {
        size_t array[] = {9, 8,  7,  6,  2/* opt */};

        MockEquivalentCompressWriter<uint32_t>::Init(array, 5);

        uint32_t buffer[] = {2, 2};
        EquivalentCompressReader<uint32_t> reader((uint8_t*)buffer);

        size_t optCompLen;        
        uint32_t itemCount = 
            EqualValueCompressAdvisor<uint32_t, MockEquivalentCompressWriter<uint32_t> >::GetOptSlotItemCount(reader, optCompLen);

        ASSERT_EQ((uint32_t)(1<<10), itemCount);
        ASSERT_EQ((size_t)2, optCompLen);
        uint32_t idx = MockEquivalentCompressWriter<uint32_t>::GetIdx();
        ASSERT_EQ((uint32_t)5, idx);
    }
}


void EqualValueCompressAdvisorTest::TestEstimateOptimizeSlotItemCount()
{
    const uint32_t itemCount = 1024 * 100;
    vector<uint32_t> dataVec(itemCount, 0);
    uint32_t value = 5;
    uint32_t expectedOptStepLen = 64;
    for (size_t i = 0; i < itemCount; i+=expectedOptStepLen)
    {
        for(size_t j=0; j<expectedOptStepLen; j++)
        {
            dataVec[i+j] = value; 
        }
        value++;
    }

    EquivalentCompressWriter<uint32_t> writer;
    writer.Init(128);
    writer.CompressData(dataVec);
    size_t compressSize = writer.GetCompressLength();
    uint8_t *buffer = new uint8_t[compressSize];
    writer.DumpBuffer(buffer, compressSize);
    EquivalentCompressReader<uint32_t> reader(buffer);

    uint32_t optStepLen = 
        EqualValueCompressAdvisor<uint32_t>::EstimateOptimizeSlotItemCount(
                reader);
    ASSERT_EQ(expectedOptStepLen, optStepLen);
    delete[] buffer;
}

void EqualValueCompressAdvisorTest::TestSampledItemIterator()
{
    const uint32_t itemCount = 1024*100+3;  // set a 1024 un-aligned buffer
    vector<uint32_t> dataVec(itemCount, 0);
    for (size_t i = 0; i < itemCount; i++)
    {
        dataVec[i] = i;
    }
    EquivalentCompressWriter<uint32_t> writer;
    writer.Init(128);
    writer.CompressData(dataVec);
    size_t compressSize = writer.GetCompressLength();
    uint8_t *buffer = new uint8_t[compressSize];
    writer.DumpBuffer(buffer, compressSize);
    EquivalentCompressReader<uint32_t> reader(buffer);

    CheckSampledItemIterator<uint32_t>(reader, 100, 0);
    CheckSampledItemIterator<uint32_t>(reader, 50, 1024);
    CheckSampledItemIterator<uint32_t>(reader, 10, 10240);
    CheckSampledItemIterator<uint32_t>(reader, 1, 0);
    delete [] buffer;
}

IE_NAMESPACE_END(index);


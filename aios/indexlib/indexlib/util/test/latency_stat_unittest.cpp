#include "indexlib/util/test/latency_stat_unittest.h"
using namespace std;

IE_NAMESPACE_BEGIN(util);
IE_LOG_SETUP(util, LatencyStatTest);

LatencyStatTest::LatencyStatTest()
{
}

LatencyStatTest::~LatencyStatTest()
{
}

void LatencyStatTest::CaseSetUp()
{
}

void LatencyStatTest::CaseTearDown()
{
}

void LatencyStatTest::TestSimpleProcess()
{
    stat.Reset();
    vector<float> pList;
    vector<uint32_t> ansList;
    // Total = 1 + 2 + 3 + ... + 99 = 4950
    for (uint32_t i = 0; i < 100; ++i)
    {
        for (uint32_t j = 0; j < i; ++j)
        {
            stat.AddSample(i);
        }
    }
    for (uint32_t i = 10; i <= 100; i+=20)
    {
        pList.push_back(i);
    }
    ASSERT_EQ(pList.size(), 5);
    stat.GetPercentile(pList, ansList);
    ASSERT_EQ(ansList.size(), 5);
    ASSERT_EQ(pList.size(), ansList.size());
    for (uint32_t i = 0, last = 0; i < 5; ++i)
    {
        ASSERT_GE(ansList[i], last);
        cerr << pList[i] << "% percentile: " << ansList[i] << endl;
        last = ansList[i];
    }
    // 4750 / 2 = 2475
    // 1 + 2 + 3 + ... + 70 = 2485
    // so median should be 70
    pList.clear();
    pList.push_back(50.0);
    stat.GetPercentile(pList, ansList);
    ASSERT_EQ(ansList[0], 70);
}

void LatencyStatTest::TestLargeDataSet()
{
    stat.Reset();
    vector<uint32_t> data;
    vector<float> pList;
    vector<uint32_t> ansList;
    const uint32_t dataSize = 1000000;
    for (uint32_t i = 0; i < dataSize ; ++i)
    {
        uint32_t lat = rand() % 20000000;
        data.push_back(lat);
        stat.AddSample(lat);
    }
    sort(data.begin(), data.end());
    for (uint32_t i = 1; i <= 1000; ++i)
    {
        pList.push_back(i / 10.0);
    }
    ASSERT_EQ(pList.size(), 1000);

    stat.GetPercentile(pList, ansList);
    ASSERT_EQ(ansList.size(), 1000);
    for (uint32_t i = 0; i < 1000; ++i)
    {
        uint32_t n = pList[i] / 100.0 * dataSize - 1;
        uint32_t error = abs((int64_t)data[n] - (int64_t)ansList[i]);
        uint32_t msb = 32 - __builtin_clz(ansList[i]) - 1;
        // deviation should be less than tolerance
        uint32_t tolerance = (1 << msb) / 64;
        // histogram's last bucket covers [16646144, +inf]
        bool flag = (error < tolerance) || (data[n] > (1 << 24));
        ASSERT_TRUE(flag) << pList[i] << "% percentile: "
                          << "Histogram Got " << ansList[i]
                          << " Actual Percentile " << data[n] << endl;
    }
}

IE_NAMESPACE_END(util);


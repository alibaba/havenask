#ifndef __INDEXLIB_EQUIVALENTCOMPRESSPERFTEST_H
#define __INDEXLIB_EQUIVALENTCOMPRESSPERFTEST_H

#include "indexlib/common_define.h"

#include "indexlib/test/test.h"
#include "indexlib/test/unittest.h"
#include "indexlib/common/numeric_compress/equivalent_compress_writer.h"
#include <autil/TimeUtility.h>

IE_NAMESPACE_BEGIN(common);

class EquivalentCompressPerfTest : public INDEXLIB_TESTBASE {
public:
    EquivalentCompressPerfTest();
    ~EquivalentCompressPerfTest();
public:
    void SetUp();
    void TearDown();
    void TestSimpleProcess();
    void TestCalculateCompressLen();

private:
    void InnerTestRead(std::string& filePath, uint32_t readTimes, uint32_t step);

    template <typename T>
    void InnerTestCalculateCompressLen(uint32_t itemCount)
    {
        std::vector<T> dataVec;
        dataVec.resize(itemCount);
        for (size_t i = 0; i < itemCount; i++)
        {
            dataVec[i] = rand();
        }

        uint32_t stepVec[] = { 64, 128, 256, 512, 1024 };
        for (size_t i = 0; i < sizeof(stepVec) / sizeof(uint32_t); i++)
        {
            std::cout << "############# itemCount:" << itemCount 
                      << ",step:" << stepVec[i] << std::endl;

            int64_t beginTime = autil::TimeUtility::currentTime();
            size_t len = EquivalentCompressWriter<T>::CalculateCompressLength(
                    dataVec, stepVec[i]);
            int64_t endTime = autil::TimeUtility::currentTime();

            std::cout << "***** compLen:" << len
                      << " ,useTime: " << (endTime - beginTime)/1000
                      << "ms." << std::endl;
        }
    }

    IE_LOG_DECLARE();
};

INDEXLIB_UNIT_TEST_CASE(EquivalentCompressPerfTest, TestSimpleProcess);
INDEXLIB_UNIT_TEST_CASE(EquivalentCompressPerfTest, TestCalculateCompressLen);

IE_NAMESPACE_END(common);

#endif //__INDEXLIB_EQUIVALENTCOMPRESSPERFTEST_H

#include <autil/StringUtil.h>
#include <autil/TimeUtility.h>
#include <unistd.h>
#include "indexlib/storage/data_flush_controller.h"

using namespace std;
using namespace autil;
using namespace fslib;
using namespace fslib::fs;

IE_NAMESPACE_BEGIN(storage);
IE_LOG_SETUP(storage, DataFlushController);

DataFlushController::DataFlushController()
    : mBaseLineTs(0)
    , mRemainQuota(std::numeric_limits<uint32_t>::max())
    , mQuotaSize(std::numeric_limits<uint32_t>::max())
    , mFlushUnit(DEFAULT_FLUSH_UNIT_SIZE)
    , mInterval(1)
    , mForceFlush(false)
    , mInited(false)
    , mTotalWaitCount(0)
{
    Reset();
    InitFromEnv();
}

DataFlushController::~DataFlushController() 
{
}

void DataFlushController::InitFromEnv()
{
    char* envParam = getenv("INDEXLIB_DATA_FLUSH_PARAM");
    if (envParam)
    {
        IE_LOG(INFO, "Init DataFlushController by env INDEXLIB_DATA_FLUSH_PARAM!");
        InitFromString(string(envParam));
    }
}

//quota_size=10485760,quota_interval=1000,flush_unit=1048576,force_flush=true
void DataFlushController::InitFromString(const string& paramStr)
{
    ScopedLock lock(mLock);
    if (mInited)
    {
        IE_LOG(INFO, "DataFlushController has already been inited!");
        return;
    }
    
    IE_LOG(INFO, "Init DataFlushController with param [%s]!", paramStr.c_str());
    vector<vector<string>> patternStrVec;
    StringUtil::fromString(paramStr, patternStrVec, "=", ",");
    for (size_t j = 0; j < patternStrVec.size(); j++)
    {
        if (patternStrVec[j].size() != 2)
        {
            IE_LOG(ERROR, "INDEXLIB_DATA_FLUSH_PARAM [%s] has format error:"
                   "inner should be kv pair format!", paramStr.c_str());
            return;
        }
        
        if (patternStrVec[j][0] == "quota_size")  // Byte
        {
            uint32_t quotaSize;
            if (StringUtil::strToUInt32(patternStrVec[j][1].c_str(), quotaSize) && quotaSize > 0)
            {
                mQuotaSize = quotaSize;
                continue;                
            }
            IE_LOG(ERROR, "quota_size [%s] must be valid number!", patternStrVec[j][1].c_str());
            return;
        }
        
        if (patternStrVec[j][0] == "flush_unit")  // Byte
        {
            uint32_t flushUnit;
            if (StringUtil::strToUInt32(patternStrVec[j][1].c_str(), flushUnit) && flushUnit > 0)
            {
                mFlushUnit = flushUnit;
                continue;                
            }
            IE_LOG(ERROR, "flush_unit [%s] must be valid number!", patternStrVec[j][1].c_str());
            return;
        }

        if (patternStrVec[j][0] == "quota_interval")  // ms
        {
            uint32_t quotaInterval;
            if (StringUtil::strToUInt32(patternStrVec[j][1].c_str(), quotaInterval))
            {
                mInterval = quotaInterval * 1000;
                continue;                
            }
            IE_LOG(ERROR, "quota_interval [%s] must be valid [ms]!", patternStrVec[j][1].c_str());
            return;
        }

        if (patternStrVec[j][0] == "force_flush")
        {
            mForceFlush = (patternStrVec[j][1] == "true" ? true : false);
            continue;
        }
        IE_LOG(ERROR, "unknown key [%s] in INDEXLIB_DATA_FLUSH_PARAM [%s]!",
               patternStrVec[j][0].c_str(), paramStr.c_str());
        return;
    }

    if (mFlushUnit > mQuotaSize)
    {
        IE_LOG(WARN, "flush_unit[%u] should be less than quota_size[%u], use quota_size instead!",
               mFlushUnit, mQuotaSize);
        mFlushUnit = mQuotaSize;
    }
    mInited = true;
}

int64_t DataFlushController::ReserveQuota(int64_t quota)
{
    assert(quota > 0);
    if (!mInited)
    {
        return quota;
    }
    
    ScopedLock lock(mLock);
    int64_t currentTs = TimeUtility::currentTime();
    if (currentTs >= mBaseLineTs + mInterval)
    {
        // new flush window
        mBaseLineTs = currentTs;
        mRemainQuota = mQuotaSize;
    }

    if (mRemainQuota <= 0)
    {
        // no quota in current flush window, wait to next
        int64_t sleepTs = mBaseLineTs + mInterval - currentTs;
        usleep(sleepTs);
        currentTs = TimeUtility::currentTime();
        mBaseLineTs = currentTs;
        mRemainQuota = mQuotaSize;

        ++mTotalWaitCount;
        if (mTotalWaitCount % 10 == 0)
        {
            IE_LOG(INFO, "wait [%lu] times for no flush quota left!", mTotalWaitCount);
        }
    }
    
    int64_t availableQuota = min(mRemainQuota, quota);
    availableQuota = min((int64_t)mFlushUnit, availableQuota);
    mRemainQuota -= availableQuota;
    return availableQuota;
}

void DataFlushController::Reset()
{
    mBaseLineTs = 0;
    mRemainQuota = std::numeric_limits<uint32_t>::max();
    mQuotaSize = std::numeric_limits<uint32_t>::max();
    mFlushUnit = DEFAULT_FLUSH_UNIT_SIZE;
    mInterval = 1;
    mForceFlush = false;
    mInited = false;
    mTotalWaitCount = 0;
}

void DataFlushController::Write(File* file, const void* buffer, size_t length)
{
    assert(file);
    size_t writeLen = 0;
    int64_t needQuota = length;
    while (needQuota > 0)
    {
        int64_t approvedQuota = ReserveQuota(needQuota);
        assert(approvedQuota > 0);
        
        ssize_t actualWriteLen = file->write((uint8_t*)buffer + writeLen, (size_t)approvedQuota);
        if (actualWriteLen <= 0)
        {
            INDEXLIB_FATAL_IO_ERROR(file->getLastError(),
                    "Write data to file: [%s] FAILED", file->getFileName());
            return;
        }
        writeLen += (size_t)actualWriteLen;
        needQuota -= actualWriteLen;
        // notice: current repeatedly write small piece data will not trigger flush operation
        if (mForceFlush && approvedQuota == mFlushUnit)
        {
            file->flush();
        }
    }
}

IE_NAMESPACE_END(storage);


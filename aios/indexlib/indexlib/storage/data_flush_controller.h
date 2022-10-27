#ifndef __INDEXLIB_DATA_FLUSH_CONTROLLER_H
#define __INDEXLIB_DATA_FLUSH_CONTROLLER_H

#include <tr1/memory>
#include <autil/Lock.h>
#include <fslib/fs/File.h>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/storage/file_wrapper.h"
#include "indexlib/misc/singleton.h"

IE_NAMESPACE_BEGIN(storage);

class DataFlushController : public misc::Singleton<DataFlushController>
{
public:
    DataFlushController();
    ~DataFlushController();
    
public:
    // ** notice: force_flush=true, not fully function, may not flush in time,
    //            suggest false when write in piece
    // format : quota_size=10485760,quota_interval=1000,flush_unit=1048576,force_flush=true
    void InitFromString(const std::string& paramStr);

public:
    void Write(fslib::fs::File* file, const void* buffer, size_t length);
    void Reset();
    
private:
    void InitFromEnv();
    int64_t ReserveQuota(int64_t quota);
    
private:
    int64_t mBaseLineTs;
    int64_t mRemainQuota;
    uint32_t mQuotaSize;
    uint32_t mFlushUnit;
    uint32_t mInterval;
    bool mForceFlush;
    bool mInited;

    size_t mTotalWaitCount;
    autil::ThreadMutex mLock;

private:
    static const uint32_t DEFAULT_FLUSH_UNIT_SIZE = 2 * 1024 * 1024; // 2 MB
    
private:
    friend class DataFlushControllerTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DataFlushController);

IE_NAMESPACE_END(storage);

#endif //__INDEXLIB_DATA_FLUSH_CONTROLLER_H

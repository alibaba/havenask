#ifndef __INDEXLIB_KV_ONLINE_CONFIG_H
#define __INDEXLIB_KV_ONLINE_CONFIG_H

#include <tr1/memory>
#include <autil/legacy/jsonizable.h>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(config);

class KVOnlineConfig : public autil::legacy::Jsonizable
{
public:
    KVOnlineConfig();
    ~KVOnlineConfig();
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    static const uint32_t INVALID_COUNT_LIMITS = std::numeric_limits<uint32_t>::max();

public:
    // for kkv
    uint32_t countLimits = INVALID_COUNT_LIMITS;
    uint32_t buildProtectThreshold = INVALID_COUNT_LIMITS;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVOnlineConfig);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KV_ONLINE_CONFIG_H

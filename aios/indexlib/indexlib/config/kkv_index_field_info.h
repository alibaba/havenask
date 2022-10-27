#ifndef __INDEXLIB_KKV_INDEX_FIELD_INFO_H
#define __INDEXLIB_KKV_INDEX_FIELD_INFO_H

#include <tr1/memory>
#include <string>
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/misc/exception.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/config/sort_param.h"
#include "indexlib/config/config_define.h"

IE_NAMESPACE_BEGIN(config);

enum KKVKeyType
{
    kkv_prefix,
    kkv_suffix,
};

class KKVIndexFieldInfo : public autil::legacy::Jsonizable
{
public:
    KKVIndexFieldInfo();
    ~KKVIndexFieldInfo() {}
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const KKVIndexFieldInfo& other) const;
    bool NeedTruncate() const { return countLimits != INVALID_COUNT_LIMITS; }

private:
    std::string KKVKeyTypeToString(KKVKeyType keyType);
    KKVKeyType StringToKKVKeyType(const std::string& str);
    
public:
    std::string fieldName;
    KKVKeyType keyType;
    uint32_t countLimits;
    uint32_t skipListThreshold;
    uint32_t protectionThreshold;
    SortParams sortParams;
    bool enableStoreOptimize;
    bool enableKeepSortSequence;
    
public:
    static const uint32_t INVALID_COUNT_LIMITS;
    static const uint32_t INVALID_SKIPLIST_THRESHOLD;
    static const uint32_t DEFAULT_PROTECTION_THRESHOLD;
private:
    IE_LOG_DECLARE();
};

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KKV_INDEX_FIELD_INFO_H

#ifndef __INDEXLIB_KV_INDEX_PREFERENCE_H
#define __INDEXLIB_KV_INDEX_PREFERENCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/misc/exception.h"
#include "indexlib/config/configurator_define.h"
#include "autil/legacy/jsonizable.h"

IE_NAMESPACE_BEGIN(config);

class KVIndexPreference : public autil::legacy::Jsonizable
{
public:
    // TODO: support hash type
    class HashDictParam : public autil::legacy::Jsonizable {
    public:
        HashDictParam(const std::string& hashType = "",
                      int32_t occupancyPct = 50,
                      bool mergeUsePreciseCount = true,
                      bool enableCompactHashKey = true,
                      bool enableShortenOffset = true)
            : mHashType(hashType)
            , mOccupancyPct(occupancyPct)
            , mMergeUsePreciseCount(mergeUsePreciseCount)
            , mEnableCompactHashKey(enableCompactHashKey)
            , mEnableShortenOffset(enableShortenOffset)
            , mMaxValueSizeForShortOffset(DEFAULT_MAX_VALUE_SIZE_FOR_SHORT_OFFSET)
        {}

        HashDictParam(const HashDictParam& other)
            : mHashType(other.mHashType)
            , mOccupancyPct(other.mOccupancyPct)
            , mMergeUsePreciseCount(other.mMergeUsePreciseCount)
            , mEnableCompactHashKey(other.mEnableCompactHashKey)
            , mEnableShortenOffset(other.mEnableShortenOffset)
            , mMaxValueSizeForShortOffset(other.mMaxValueSizeForShortOffset)
        {}

        const std::string& GetHashType() const { return mHashType; }
        int32_t GetOccupancyPct() const { return mOccupancyPct; }
        
        virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
        {
            json.Jsonize("type", mHashType, mHashType);
            json.Jsonize("occupancy_pct", mOccupancyPct, mOccupancyPct);
            json.Jsonize("merge_use_precise_count", mMergeUsePreciseCount, mMergeUsePreciseCount);
            json.Jsonize("enable_compact_hash_key", mEnableCompactHashKey, mEnableCompactHashKey);
            json.Jsonize("enable_shorten_offset", mEnableShortenOffset, mEnableShortenOffset);
            // for test
            json.Jsonize("max_value_size_for_short_offset", mMaxValueSizeForShortOffset,
                         mMaxValueSizeForShortOffset);
        }
        void Check() const
        {
            if (mHashType.empty())
            {
                return;
            }
            if (mHashType != "dense" && mHashType != "separate_chain"
                && mHashType != "cuckoo")
            {
                INDEXLIB_FATAL_ERROR(BadParameter,
                        "unsupported hash dict type [%s]", mHashType.c_str());
            }
            if (mOccupancyPct <= 0 || mOccupancyPct > 100)
            {
                INDEXLIB_FATAL_ERROR(BadParameter, "invalid occupancy_pct[%d]",
                        mOccupancyPct);
            }
        }

        bool UsePreciseCountWhenMerge() const { return mMergeUsePreciseCount; }
        bool HasEnableCompactHashKey() const { return mEnableCompactHashKey; }
        bool HasEnableShortenOffset() const { return mEnableShortenOffset; }

        void SetEnableCompactHashKey(bool flag) {  mEnableCompactHashKey = flag; }
        void SetEnableShortenOffset(bool flag) { mEnableShortenOffset = flag; }

        void SetMaxValueSizeForShortOffset(size_t size) { mMaxValueSizeForShortOffset = size; }
        size_t GetMaxValueSizeForShortOffset() const { return mMaxValueSizeForShortOffset; }
    private:
        std::string mHashType;
        int32_t mOccupancyPct;
        bool mMergeUsePreciseCount;
        bool mEnableCompactHashKey;
        bool mEnableShortenOffset;
        size_t mMaxValueSizeForShortOffset;
    };

    class ValueParam : public autil::legacy::Jsonizable
    {
    public:
        ValueParam(bool encode = false,
                   const std::string& fileCompType = "",
                   size_t fileCompBuffSize = DEFAULT_FILE_COMP_BUFF_SIZE,
                   bool fixLenAutoInline = false)
            : mEncode(encode)
            , mFileCompType(fileCompType)
            , mFileCompBuffSize(fileCompBuffSize)
            , mFixLenAutoInline(fixLenAutoInline)
        {}
        
        bool IsEncode() const { return mEncode; }

        bool EnableFileCompress() const { return !mFileCompType.empty(); }
        const std::string& GetFileCompressType() const { return mFileCompType; }
        size_t GetFileCompressBufferSize() const { return mFileCompBuffSize; }
        void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
        void Check() const;
        bool IsFixLenAutoInline() const { return mFixLenAutoInline; }
        void EnableFixLenAutoInline() { mFixLenAutoInline = true; }
    private:
        static const size_t DEFAULT_FILE_COMP_BUFF_SIZE = 4 * 1024;
        
    private:
        bool mEncode;
        std::string mFileCompType;
        size_t mFileCompBuffSize;
        bool mFixLenAutoInline;
    };

public:
    KVIndexPreference();
    virtual ~KVIndexPreference();
    
public:
    IndexPreferenceType GetType() const { return mType; }
    const HashDictParam& GetHashDictParam() const { return mHashParam; }
    const ValueParam& GetValueParam() const { return mValueParam; }

    // for test
    void SetType(IndexPreferenceType type) { mType = type; }
    void SetHashDictParam(const HashDictParam& param) { mHashParam = param; }
    void SetValueParam(const ValueParam& param) { mValueParam = param; }
    
public:
    virtual void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    virtual void Check() const;

protected:
    IndexPreferenceType StrToPreferenceType(const std::string& typeStr) const;
    std::string PreferenceTypeToStr(IndexPreferenceType type) const;

private:
    void InitDefaultParams(IndexPreferenceType type);

protected:
    IndexPreferenceType mType;
    HashDictParam mHashParam;
    ValueParam mValueParam;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVIndexPreference);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KV_INDEX_PREFERENCE_H

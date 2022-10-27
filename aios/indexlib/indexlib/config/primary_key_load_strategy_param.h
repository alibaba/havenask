#ifndef __INDEXLIB_PRIMARY_KEY_LOAD_STRATEGY_PARAM_H
#define __INDEXLIB_PRIMARY_KEY_LOAD_STRATEGY_PARAM_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"

IE_NAMESPACE_BEGIN(config);

//"combine_segments=true,max_doc_count=3"
class PrimaryKeyLoadStrategyParam
{
public:
    enum PrimaryKeyLoadMode
    {
        SORTED_VECTOR = 0,
        HASH_TABLE
    };

public:
    PrimaryKeyLoadStrategyParam(PrimaryKeyLoadMode mode = SORTED_VECTOR,
                                bool lookupReverse = true)
        : mLoadMode(mode)
        , mMaxDocCount(0)
        , mNeedCombineSegments(false)
        , mNeedLookupReverse(lookupReverse)
    {}

    ~PrimaryKeyLoadStrategyParam() {}

public:
    void FromString(const std::string& paramStr);
    PrimaryKeyLoadMode GetPrimaryKeyLoadMode() const { return mLoadMode; }
    size_t GetMaxDocCount() const { return mMaxDocCount; }
    bool NeedCombineSegments() const { return mNeedCombineSegments; }
    void AssertEqual(const PrimaryKeyLoadStrategyParam& other) const;
    static void CheckParam(const std::string& param);
    bool NeedLookupReverse() const { return mNeedLookupReverse; }

private:
    bool ParseParams(const std::vector<std::vector<std::string> >& params);
    bool ParseParam(const std::string& name, const std::string& value);

private:
    static const std::string COMBINE_SEGMENTS;
    static const std::string PARAM_SEP;
    static const std::string KEY_VALUE_SEP;
    static const std::string MAX_DOC_COUNT;
    static const std::string LOOKUP_REVERSE;

private:
    PrimaryKeyLoadMode mLoadMode;
    size_t mMaxDocCount;
    bool mNeedCombineSegments;
    bool mNeedLookupReverse;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(PrimaryKeyLoadStrategyParam);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_PRIMARY_KEY_LOAD_STRATEGY_PARAM_H

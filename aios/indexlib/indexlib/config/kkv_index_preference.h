#ifndef __INDEXLIB_KKV_INDEX_PREFERENCE_H
#define __INDEXLIB_KKV_INDEX_PREFERENCE_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include "indexlib/config/kv_index_preference.h"

IE_NAMESPACE_BEGIN(config);

class KKVIndexPreference : public KVIndexPreference
{
public:
    typedef KVIndexPreference::HashDictParam HashDictParam;
    typedef KVIndexPreference::ValueParam ValueParam;
    typedef ValueParam SuffixKeyParam;

public:
    KKVIndexPreference();
    ~KKVIndexPreference();

public:
    const SuffixKeyParam& GetSkeyParam() const { return mSkeyParam; }
    void SetSkeyParam(const SuffixKeyParam& param) { mSkeyParam = param; }
    bool IsValueInline() const { return mValueInline; }

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const override;

private:
    void InitDefaultParams(IndexPreferenceType type);

private:
    SuffixKeyParam mSkeyParam;
    bool mValueInline;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KKVIndexPreference);

IE_NAMESPACE_END(config);

#endif //__INDEXLIB_KKV_INDEX_PREFERENCE_H

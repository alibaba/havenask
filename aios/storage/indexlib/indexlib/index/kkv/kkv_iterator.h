/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_KKV_ITERATOR_H
#define __INDEXLIB_KKV_ITERATOR_H

#include <memory>

#include "autil/mem_pool/Pool.h"
#include "indexlib/common/field_format/pack_attribute/plain_format_encoder.h"
#include "indexlib/common_define.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/index/kkv/cached_kkv_iterator_impl.h"
#include "indexlib/index/kkv/normal_kkv_iterator_impl.h"
#include "indexlib/index/kv/kv_metrics_collector.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class KKVIterator
{
public:
    KKVIterator(autil::mem_pool::Pool* pool, KKVIteratorImplBase* iter, FieldType mSKeyDictFieldType,
                config::KKVIndexConfig* kkvConfig, KVMetricsCollector* metricsCollector,
                common::PlainFormatEncoder* plainFormatEncoder, uint32_t skeyCountLimit);

    ~KKVIterator();

public:
    bool IsValid() const { return !mResultBuffer.IsEof(); }
    void MoveToNext()
    {
        mResultBuffer.MoveToNext();
        if (mResultBuffer.IsEof()) {
            return;
        }
        if (mResultBuffer.IsValid()) {
            return;
        }
        mResultBuffer.Clear();
        mIterator->FillResultBuffer(mResultBuffer);
    }
    void GetCurrentValue(autil::StringView& value)
    {
        mResultBuffer.GetCurrentValue(value);
        if (mPlainFormatEncoder && !mResultBuffer.IsCurrentDocInCache()) { // in cache doc already decoded
            bool ret = mPlainFormatEncoder->Decode(value, mPool, value);
            assert(ret);
            (void)ret;
        }
    }
    uint64_t GetCurrentTimestamp() { return SecondToMicrosecond(mResultBuffer.GetCurrentTimestamp()); }
    uint64_t GetCurrentSkey() { return mResultBuffer.GetCurrentSkey(); }

    // if skey value is same with skey hash value, skey will not store in value
    bool IsOptimizeStoreSKey() const { return mKKVConfig->OptimizedStoreSKey(); }
    const std::string& GetSKeyFieldName() const { return mKKVConfig->GetSuffixFieldName(); }
    void Finish()
    {
        if (mMetricsCollector) {
            mMetricsCollector->EndQuery();
        }
    }
    bool IsSorted() const { return mIterator->IsSorted(); }
    regionid_t GetRegionId() const { return mKKVConfig->GetRegionId(); }

public:
    std::string TEST_GetSKeyValueStr();
    FieldType TEST_GetSKeyDictFieldType() const { return mSKeyDictFieldType; }
    void TEST_collectCodegenResult(codegen::CodegenObject::CodegenCheckers& checkers)
    {
        mIterator->TEST_collectCodegenResult(checkers, "");
    }

private:
    autil::mem_pool::Pool* mPool;
    KKVIteratorImplBase* mIterator;
    config::KKVIndexConfig* mKKVConfig;
    KKVResultBuffer mResultBuffer;
    KVMetricsCollector* mMetricsCollector;
    common::PlainFormatEncoder* mPlainFormatEncoder;
    FieldType mSKeyDictFieldType;
};

DEFINE_SHARED_PTR(KKVIterator);

////////////////////////////////////////////////////////////

inline std::string KKVIterator::TEST_GetSKeyValueStr()
{
    assert(IsOptimizeStoreSKey());
    uint64_t skey = GetCurrentSkey();
    FieldType fieldType = mKKVConfig->GetSuffixFieldConfig()->GetFieldType();
    switch (fieldType) {
#define MACRO(type)                                                                                                    \
    case type:                                                                                                         \
        return autil::StringUtil::toString<config::FieldTypeTraits<type>::AttrItemType>(skey);

        NUMBER_FIELD_MACRO_HELPER(MACRO)
#undef MACRO
    default:
        assert(false);
    }
    return std::string("");
}
}} // namespace indexlib::index

#endif //__INDEXLIB_KKV_ITERATOR_H

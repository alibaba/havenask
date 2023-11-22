#include "indexlib/table/kkv_table/test/KKVTableTestSearcher.h"

#include "autil/mem_pool/Pool.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kkv/common/Trait.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/table/test/DocumentParser.h"
#include "indexlib/table/test/RawDocument.h"
#include "indexlib/table/test/Result.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlibv2::index;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVTableTestSearcher);

KKVTableTestSearcher::KKVTableTestSearcher() {}

KKVTableTestSearcher::~KKVTableTestSearcher() {}

void KKVTableTestSearcher::Init(const std::shared_ptr<indexlibv2::table::KKVReader>& reader,
                                const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig)
{
    _indexConfig = indexConfig;
    _kkvReader = reader;
    _valueConfig = _indexConfig->GetValueConfig();

    _packAttrFormatter.reset(new PackAttributeFormatter);
    auto [status, packAttrConfig] = _valueConfig->CreatePackAttributeConfig();
    assert(status.IsOK());
    if (!_packAttrFormatter->Init(packAttrConfig)) {
        assert(false);
    }
}

std::shared_ptr<indexlibv2::table::Result> KKVTableTestSearcher::Search(const std::string& pkeyStr,
                                                                        const std::vector<std::string>& skeyStrs,
                                                                        indexlibv2::table::KKVReadOptions& readOptions)
{
    std::vector<autil::StringView> skeys;
    skeys.reserve(skeyStrs.size());
    for (const std::string& skeyStr : skeyStrs) {
        skeys.push_back(skeyStr);
    }
    autil::StringView pkey = pkeyStr;

    std::shared_ptr<indexlibv2::table::Result> result(new indexlibv2::table::Result());

    auto kkvDocIter = future_lite::interface::syncAwait(_kkvReader->LookupAsync(pkey, skeys, readOptions), &_executor);

    if (!kkvDocIter) {
        result->SetError(true);
    }
    while (kkvDocIter && kkvDocIter->IsValid()) {
        FillResult(result, kkvDocIter, readOptions);
        kkvDocIter->MoveToNext();
    }
    if (kkvDocIter) {
        kkvDocIter->Finish();
    }
    indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(readOptions.pool, kkvDocIter);
    return result;
}

std::shared_ptr<indexlibv2::table::Result>
KKVTableTestSearcher::Search(const std::vector<std::string>& pkeyStrVec,
                             const std::vector<std::vector<std::string>>& skeyStrsVec,
                             indexlibv2::table::KKVReadOptions& readOptions, bool batchFinish)
{
    vector<autil::StringView> pkeyVec;
    std::vector<std::vector<autil::StringView>> skeysVec;
    for (const auto& pkeyStr : pkeyStrVec) {
        autil::StringView pkey = pkeyStr;
        pkeyVec.push_back(pkey);
    }

    for (auto& skeyStrs : skeyStrsVec) {
        std::vector<autil::StringView> skeys;
        skeys.reserve(skeyStrs.size());
        for (const auto& skeyStr : skeyStrs) {
            skeys.push_back(skeyStr);
        }
        skeysVec.push_back(skeys);
    }

    std::shared_ptr<indexlibv2::table::Result> result(new indexlibv2::table::Result());

    auto kkvResult = future_lite::interface::syncAwait(
        _kkvReader->BatchLookupAsync(pkeyVec.begin(), pkeyVec.end(), skeysVec.begin(), skeysVec.end(), readOptions),
        &_executor);

#if FUTURE_LITE_USE_COROUTINES
    {
        for (auto iter = kkvResult->Begin(); iter != kkvResult->End(); iter++) {
            auto& lazyKKVDocIter = *iter;
            if (lazyKKVDocIter.hasError()) {
                result->SetError(true);
                continue;
            }
            auto& kkvDocIter = lazyKKVDocIter.value();
            if (!kkvDocIter) {
                result->SetError(true);
                continue;
            }
            while (kkvDocIter && kkvDocIter->IsValid()) {
                FillResult(result, kkvDocIter, readOptions);
                kkvDocIter->MoveToNext();
            }
        }
    }
#else
    {
        for (auto iter = kkvResult->Begin(); iter != kkvResult->End(); iter++) {
            auto& kkvDocIter = *iter;
            if (!kkvDocIter) {
                result->SetError(true);
            }
        }

        for (auto iter = kkvResult->Begin(); iter != kkvResult->End(); iter++) {
            auto& kkvDocIter = *iter;
            while (kkvDocIter && kkvDocIter->IsValid()) {
                FillResult(result, kkvDocIter, readOptions);
                kkvDocIter->MoveToNext();
            }
        }
    }
#endif
    if (batchFinish) {
        kkvResult->Finish();
    } else {
        for (size_t i = 0; i < kkvResult->GetKKVIteratorSize(); i++) {
            kkvResult->Finish(i);
        }
    }

    indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(readOptions.pool, kkvResult);

    return result;
}

void KKVTableTestSearcher::FillResult(const std::shared_ptr<indexlibv2::table::Result>& result, KKVIterator* iter,
                                      indexlibv2::table::KKVReadOptions& readOptions)
{
    StringView value;
    iter->GetCurrentValue(value);
    uint64_t timestamp = iter->GetCurrentTimestamp();

    std::shared_ptr<RawDocument> rawDoc(new RawDocument);
    vector<string> subAttrs;
    auto [status, packAttrConfig] = _valueConfig->CreatePackAttributeConfig();
    assert(status.IsOK());
    assert(packAttrConfig);

    const auto& subAttrConfigVec = packAttrConfig->GetAttributeConfigVec();
    for (const auto& subAttr : subAttrConfigVec) {
        const string& attrName = subAttr->GetAttrName();
        AttributeReference* attrRef = _packAttrFormatter->GetAttributeReference(attrName);
        if (!attrRef) {
            continue;
        }
        string fieldValue;
        attrRef->GetStrValue(value.data(), fieldValue, readOptions.pool);
        if (subAttr->IsMultiValue()) {
            fieldValue = DocumentParser::ParseMultiValueResult(fieldValue);
        }
        rawDoc->SetField(attrName, fieldValue);
    }

    if (iter->IsOptimizeStoreSKey()) {
        rawDoc->SetField(iter->GetSKeyFieldName(), GetSKeyValueStr(iter->GetCurrentSkey()));
    }
    rawDoc->SetField(RESERVED_TIMESTAMP, std::to_string(timestamp));
    result->AddDoc(rawDoc);
}

std::string KKVTableTestSearcher::GetSKeyValueStr(uint64_t skey)
{
    FieldType skeyDictType = _indexConfig->GetSuffixFieldConfig()->GetFieldType();
    switch (skeyDictType) {
#define CASE_MACRO(type)                                                                                               \
    case type:                                                                                                         \
        return autil::StringUtil::toString<indexlib::index::FieldTypeTraits<type>::AttrItemType>(skey);
        KKV_SKEY_DICT_TYPE_MACRO_HELPER(CASE_MACRO);
#undef CASE_MACRO
    default: {
        AUTIL_LOG(ERROR, "unsupported skey field_type:[%s]", config::FieldConfig::FieldTypeToStr(skeyDictType));
        assert(false);
        return std::string("");
    }
    }
}

} // namespace indexlibv2::table

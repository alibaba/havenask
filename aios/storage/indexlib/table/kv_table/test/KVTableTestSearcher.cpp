#include "indexlib/table/kv_table/test/KVTableTestSearcher.h"

#include "autil/mem_pool/Pool.h"
#include "future_lite/coro/Lazy.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/pack_attribute/PackAttributeFormatter.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/KVReadOptions.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"
#include "indexlib/index/kv/config/ValueConfig.h"
#include "indexlib/table/test/DocumentParser.h"
#include "indexlib/table/test/RawDocument.h"
#include "indexlib/table/test/Result.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;
using namespace indexlibv2::index;

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KVTableTestSearcher);

KVTableTestSearcher::KVTableTestSearcher() {}

KVTableTestSearcher::~KVTableTestSearcher() {}

void KVTableTestSearcher::Init(std::shared_ptr<indexlibv2::index::KVIndexReader> reader,
                               std::shared_ptr<indexlibv2::config::KVIndexConfig> kvIndexConfig)
{
    _kvReader = reader;
    _valueConfig = kvIndexConfig->GetValueConfig();
    if (_valueConfig->GetAttributeCount() == 1 && !_valueConfig->GetAttributeConfig(0)->IsMultiValue() &&
        (_valueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string)) {
        return;
    }
    _packAttrFormatter.reset(new PackAttributeFormatter);
    auto [status, packAttrConfig] = _valueConfig->CreatePackAttributeConfig();
    assert(status.IsOK());
    if (!_packAttrFormatter->Init(packAttrConfig)) {
        assert(false);
    }
}

std::shared_ptr<indexlibv2::table::Result> KVTableTestSearcher::Search(const string& keyStr, uint64_t timestamp,
                                                                       indexlib::TableSearchCacheType searchCacheType)
{
    Pool pool;
    StringView key(keyStr);
    std::shared_ptr<indexlibv2::table::Result> result(new indexlibv2::table::Result());
    indexlibv2::index::KVReadOptions options;
    options.timestamp = timestamp;
    options.pool = &pool;
    auto kvResult = future_lite::interface::syncAwait(_kvReader->GetAsync(key, options));
    if (kvResult.status != indexlibv2::index::KVResultStatus::FOUND) {
        return result;
    }
    FillResult(result, kvResult.valueExtractor);
    return result;
}

void KVTableTestSearcher::FillResult(const std::shared_ptr<indexlibv2::table::Result>& result,
                                     const indexlibv2::index::FieldValueExtractor& valueExtractor)
{
    std::shared_ptr<RawDocument> rawDoc(new RawDocument);
    size_t fieldCount = valueExtractor.GetFieldCount();
    for (size_t i = 0; i < fieldCount; i++) {
        std::string fieldName;
        FieldType fieldType;
        bool isMultiValue;
        std::string fieldValue;
        if (!valueExtractor.GetValueMeta(i, fieldName, fieldType, isMultiValue) ||
            !valueExtractor.GetStringValue(i, fieldValue)) {
            continue;
        }

        if (isMultiValue) {
            fieldValue = DocumentParser::ParseMultiValueResult(fieldValue);
        }
        rawDoc->SetField(fieldName, fieldValue);
    }
    result->AddDoc(rawDoc);
}

} // namespace indexlibv2::table

#include "indexlib/test/kv_searcher.h"

#include "autil/StringUtil.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/config/field_type_traits.h"
#include "indexlib/config/kv_index_config.h"
#include "indexlib/config/value_config.h"
#include "indexlib/index/kv/FieldValueExtractor.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/kv_reader.h"
#include "indexlib/partition/index_partition_reader.h"
#include "indexlib/test/document_parser.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::index;
using namespace indexlib::partition;
using namespace indexlib::common;
using namespace indexlib::config;

namespace indexlib { namespace test {
IE_LOG_SETUP(test, KVSearcher);

KVSearcher::KVSearcher() {}

KVSearcher::~KVSearcher() {}

void KVSearcher::Init(const IndexPartitionReaderPtr& reader, IndexPartitionSchema* schema, const string& regionName)
{
    assert(reader);
    assert(schema);

    regionid_t regionId = schema->GetRegionId(regionName);
    mReader = reader->GetKVReader(regionId);
    assert(mReader);
    const IndexSchemaPtr& indexSchema = schema->GetIndexSchema(regionId);
    assert(indexSchema);
    KVIndexConfigPtr kvIndexConfig = DYNAMIC_POINTER_CAST(KVIndexConfig, indexSchema->GetPrimaryKeyIndexConfig());
    assert(kvIndexConfig);
    mValueConfig = kvIndexConfig->GetValueConfig();
    assert(mValueConfig);
    if (schema->GetRegionCount() == 1 && mValueConfig->GetAttributeCount() == 1 &&
        !mValueConfig->GetAttributeConfig(0)->IsMultiValue() &&
        (mValueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string)) {
        return;
    }
    mPackAttrFormatter.reset(new PackAttributeFormatter);
    if (!mPackAttrFormatter->Init(mValueConfig->CreatePackAttributeConfig())) {
        assert(false);
    }
}

void KVSearcher::Init(std::shared_ptr<indexlibv2::index::KVIndexReader> reader,
                      std::shared_ptr<indexlib::config::KVIndexConfig> kvIndexConfig)
{
    _kvReader = reader;
    mValueConfig = kvIndexConfig->GetValueConfig();
    if (mValueConfig->GetAttributeCount() == 1 && !mValueConfig->GetAttributeConfig(0)->IsMultiValue() &&
        (mValueConfig->GetAttributeConfig(0)->GetFieldType() != ft_string)) {
        return;
    }
    mPackAttrFormatter.reset(new PackAttributeFormatter);
    if (!mPackAttrFormatter->Init(mValueConfig->CreatePackAttributeConfig())) {
        assert(false);
    }
}

ResultPtr KVSearcher::Search(const string& keyStr, uint64_t timestamp, TableSearchCacheType searchCacheType)
{
    Pool pool;
    StringView key(keyStr);
    ResultPtr result(new test::Result());
    StringView value;
    if (mReader) {
        if (!future_lite::interface::syncAwait(mReader->GetAsync(key, value, timestamp, searchCacheType, &pool))) {
            return result;
        }
        if (mPackAttrFormatter) {
            FillPackResult(result, value, &pool);
            return result;
        } else {
            RawDocumentPtr rawDoc(new RawDocument());
            string str = ConvertValueToString(mValueConfig->GetAttributeConfig(0), value);
            rawDoc->SetField(mValueConfig->GetAttributeConfig(0)->GetAttrName(), str);
            result->AddDoc(rawDoc);
        }
    } else {
        indexlibv2::index::KVReadOptions options;
        options.timestamp = timestamp;
        options.pool = &pool;
        auto kvResult = future_lite::interface::syncAwait(_kvReader->GetAsync(key, options));
        if (kvResult.status != indexlibv2::index::KVResultStatus::FOUND) {
            return result;
        }
        FillResult(result, kvResult.valueExtractor);
    }
    return result;
}

string KVSearcher::ConvertValueToString(const AttributeConfigPtr& attrConfig, const StringView& value)
{
    string valueStr;
    if (attrConfig->IsMultiValue()) {
        assert(false);
        // TODO support
        return valueStr;
    }
    switch (attrConfig->GetFieldType()) {
#define MACRO(ft)                                                                                                      \
    case ft: {                                                                                                         \
        typedef FieldTypeTraits<ft>::AttrItemType type;                                                                \
        assert(value.size() == sizeof(type));                                                                          \
        valueStr = StringUtil::toString(*(type*)value.data());                                                         \
        return valueStr;                                                                                               \
    }
        NUMBER_FIELD_MACRO_HELPER(MACRO)
    default: {
        assert(false);
    }
#undef MACRO
    }

    return valueStr;
}

void KVSearcher::FillPackResult(const ResultPtr& result, const StringView& value, Pool* pool)
{
    if (value.size() == 0) {
        return;
    }

    RawDocumentPtr rawDoc(new RawDocument);
    assert(mPackAttrFormatter);
    PackAttributeConfigPtr packAttrConfig = mValueConfig->CreatePackAttributeConfig();
    assert(packAttrConfig);
    const std::vector<AttributeConfigPtr>& subAttrConfigVec = packAttrConfig->GetAttributeConfigVec();
    for (const auto& subAttr : subAttrConfigVec) {
        const string& attrName = subAttr->GetAttrName();
        AttributeReference* attrRef = mPackAttrFormatter->GetAttributeReference(attrName);
        if (!attrRef) {
            continue;
        }
        string fieldValue;
        attrRef->GetStrValue(value.data(), fieldValue, pool);
        if (subAttr->IsMultiValue()) {
            fieldValue = DocumentParser::ParseMultiValueResult(fieldValue);
        }
        rawDoc->SetField(attrName, fieldValue);
    }
    result->AddDoc(rawDoc);
}

void KVSearcher::FillResult(const ResultPtr& result, const indexlibv2::index::FieldValueExtractor& valueExtractor)
{
    RawDocumentPtr rawDoc(new RawDocument);
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

}} // namespace indexlib::test

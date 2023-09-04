#include "indexlib/index/kv/test/KVIndexConfigBuilder.h"

#include "indexlib/index/kv/config/KVIndexPreference.h"
#include "indexlib/table/kv_table/test/KVTabletSchemaMaker.h"

namespace indexlibv2::index {

KVIndexConfigBuilder::KVIndexConfigBuilder(const std::string& fieldNames, const std::string& keyName,
                                           const std::string& valueNames, int64_t ttl)
{
    _schema = table::KVTabletSchemaMaker::Make(fieldNames, keyName, valueNames, ttl);
    if (_schema) {
        auto indexConfig = _schema->GetIndexConfig("kv", keyName);
        if (indexConfig) {
            _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::KVIndexConfig>(indexConfig);
        }
    }
}

KVIndexConfigBuilder::~KVIndexConfigBuilder() {}

KVIndexConfigBuilder& KVIndexConfigBuilder::SetTTL(int64_t ttl)
{
    _indexConfig->SetTTL(ttl);
    return *this;
}

KVIndexConfigBuilder& KVIndexConfigBuilder::SetHashType(const std::string& hashType)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetHashType(hashType);
    return *this;
}

KVIndexConfigBuilder& KVIndexConfigBuilder::SetOccupancyPct(int32_t occupancyPct)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetOccupancyPct(occupancyPct);
    return *this;
}

KVIndexConfigBuilder& KVIndexConfigBuilder::SetEnableCompactHashKey(bool flag)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetEnableCompactHashKey(flag);
    return *this;
}

KVIndexConfigBuilder& KVIndexConfigBuilder::SetEnableShortenOffset(bool flag)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetEnableShortenOffset(flag);
    return *this;
}

KVIndexConfigBuilder& KVIndexConfigBuilder::SetMaxValueSizeForShortOffset(size_t size)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetMaxValueSizeForShortOffset(size);
    return *this;
}

KVIndexConfigBuilder&
KVIndexConfigBuilder::SetValueCompressionParameters(const std::string& compressorType,
                                                    const std::map<std::string, std::string>& params)
{
    auto& valueParam = _indexConfig->GetIndexPreference().GetValueParam();
    valueParam.SetFileCompressType(compressorType);
    for (const auto& it : params) {
        valueParam.SetCompressParameter(it.first, it.second);
    }
    return *this;
}

KVIndexConfigBuilder& KVIndexConfigBuilder::SetEnableFixLenAutoInline(bool flag)
{
    auto& valueParam = _indexConfig->GetIndexPreference().GetValueParam();
    valueParam.SetEnableFixLenAutoInline(flag);
    return *this;
}

KVIndexConfigBuilder& KVIndexConfigBuilder::SetValueFormat(const std::string& valueFormat)
{
    auto indexPreference = _indexConfig->GetIndexPreference();
    auto& valueParam = indexPreference.GetValueParam();
    if (valueFormat == "plain") {
        valueParam.EnablePlainFormat(true);
    } else if (valueFormat == "impact") {
        valueParam.EnableValueImpact(true);
    } else {
        valueParam.EnablePlainFormat(false);
        valueParam.EnableValueImpact(false);
    }
    _indexConfig->SetIndexPreference(indexPreference);
    return *this;
}

KVIndexConfigBuilder::Result KVIndexConfigBuilder::MakeIndexConfig(const std::string& fields, const std::string& key,
                                                                   const std::string& values, int64_t ttl)
{
    KVIndexConfigBuilder builder(fields, key, values, ttl);
    return builder.Finalize();
}

} // namespace indexlibv2::index

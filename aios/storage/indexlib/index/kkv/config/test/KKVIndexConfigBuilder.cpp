#include "indexlib/index/kkv/config/test/KKVIndexConfigBuilder.h"

#include "indexlib/index/kkv/config/KKVIndexPreference.h"
#include "indexlib/table/kkv_table/test/KKVTabletSchemaMaker.h"

namespace indexlibv2::index {

KKVIndexConfigBuilder::KKVIndexConfigBuilder(const std::string& fieldNames, const std::string& pkeyName,
                                             const std::string& skeyName, const std::string& valueNames, int64_t ttl)
{
    _schema = table::KKVTabletSchemaMaker::Make(fieldNames, pkeyName, skeyName, valueNames, ttl);
    if (_schema) {
        auto indexConfig = _schema->GetIndexConfig("kkv", pkeyName);
        if (indexConfig) {
            _indexConfig = std::dynamic_pointer_cast<indexlibv2::config::KKVIndexConfig>(indexConfig);
        }
    }
}

KKVIndexConfigBuilder::~KKVIndexConfigBuilder() {}

KKVIndexConfigBuilder& KKVIndexConfigBuilder::SetTTL(int64_t ttl)
{
    _indexConfig->SetTTL(ttl);
    return *this;
}

KKVIndexConfigBuilder& KKVIndexConfigBuilder::SetHashType(const std::string& hashType)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetHashType(hashType);
    return *this;
}

KKVIndexConfigBuilder& KKVIndexConfigBuilder::SetOccupancyPct(int32_t occupancyPct)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetOccupancyPct(occupancyPct);
    return *this;
}

KKVIndexConfigBuilder& KKVIndexConfigBuilder::SetEnableCompactHashKey(bool flag)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetEnableCompactHashKey(flag);
    return *this;
}

KKVIndexConfigBuilder& KKVIndexConfigBuilder::SetEnableShortenOffset(bool flag)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetEnableShortenOffset(flag);
    return *this;
}

KKVIndexConfigBuilder& KKVIndexConfigBuilder::SetMaxValueSizeForShortOffset(size_t size)
{
    auto& hashDictParam = _indexConfig->GetIndexPreference().GetHashDictParam();
    hashDictParam.SetMaxValueSizeForShortOffset(size);
    return *this;
}

KKVIndexConfigBuilder&
KKVIndexConfigBuilder::SetValueCompressionParameters(const std::string& compressorType,
                                                     const std::map<std::string, std::string>& params)
{
    auto& valueParam = _indexConfig->GetIndexPreference().GetValueParam();
    valueParam.SetFileCompressType(compressorType);
    for (const auto& it : params) {
        valueParam.SetCompressParameter(it.first, it.second);
    }
    return *this;
}

KKVIndexConfigBuilder& KKVIndexConfigBuilder::SetEnableFixLenAutoInline(bool flag)
{
    auto& valueParam = _indexConfig->GetIndexPreference().GetValueParam();
    valueParam.SetEnableFixLenAutoInline(flag);
    return *this;
}

KKVIndexConfigBuilder& KKVIndexConfigBuilder::SetValueFormat(const std::string& valueFormat)
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

KKVIndexConfigBuilder::Result KKVIndexConfigBuilder::MakeIndexConfig(const std::string& fields, const std::string& pkey,
                                                                     const std::string& skey, const std::string& values,
                                                                     int64_t ttl)
{
    KKVIndexConfigBuilder builder(fields, pkey, skey, values, ttl);
    return builder.Finalize();
}

} // namespace indexlibv2::index

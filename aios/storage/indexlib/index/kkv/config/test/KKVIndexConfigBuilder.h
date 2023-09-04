#pragma once

#include <map>
#include <memory>

#include "indexlib/config/ITabletSchema.h"
#include "indexlib/index/kkv/config/KKVIndexConfig.h"

namespace indexlibv2::index {

class KKVIndexConfigBuilder
{
public:
    using SchemaType = std::shared_ptr<indexlibv2::config::ITabletSchema>;
    using IndexConfigType = std::shared_ptr<indexlibv2::config::KKVIndexConfig>;
    using Result = std::pair<SchemaType, IndexConfigType>;

public:
    explicit KKVIndexConfigBuilder(const std::string& fieldNames, const std::string& pkeyName,
                                   const std::string& skeyName, const std::string& valueNames, int64_t ttl = -1);
    ~KKVIndexConfigBuilder();

public:
    bool Valid() const { return _schema && _indexConfig; }

    KKVIndexConfigBuilder& SetTTL(int64_t ttl);

    KKVIndexConfigBuilder& SetHashType(const std::string& hashType);
    KKVIndexConfigBuilder& SetOccupancyPct(int32_t occupancyPct);
    KKVIndexConfigBuilder& SetEnableCompactHashKey(bool flag);
    KKVIndexConfigBuilder& SetEnableShortenOffset(bool flag);
    KKVIndexConfigBuilder& SetMaxValueSizeForShortOffset(size_t size);
    KKVIndexConfigBuilder& SetValueFormat(const std::string& valueFormat);
    KKVIndexConfigBuilder& SetValueCompressionParameters(const std::string& compressorType,
                                                         const std::map<std::string, std::string>& params = {});
    KKVIndexConfigBuilder& SetEnableFixLenAutoInline(bool flag);

    Result Finalize() { return {std::move(_schema), std::move(_indexConfig)}; }

public:
    static Result MakeIndexConfig(const std::string& fieldNames, const std::string& pkeyName,
                                  const std::string& skeyName, const std::string& valueNames, int64_t ttl);

private:
    SchemaType _schema;
    IndexConfigType _indexConfig;
};

} // namespace indexlibv2::index

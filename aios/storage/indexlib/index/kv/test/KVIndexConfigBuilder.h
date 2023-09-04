#pragma once

#include <map>
#include <memory>

#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/kv/config/KVIndexConfig.h"

namespace indexlibv2::index {

class KVIndexConfigBuilder
{
public:
    explicit KVIndexConfigBuilder(const std::string& fieldNames, const std::string& keyName,
                                  const std::string& valueNames, int64_t ttl = -1);
    ~KVIndexConfigBuilder();

public:
    bool Valid() const { return _schema && _indexConfig; }

    KVIndexConfigBuilder& SetTTL(int64_t ttl);

    KVIndexConfigBuilder& SetHashType(const std::string& hashType);
    KVIndexConfigBuilder& SetOccupancyPct(int32_t occupancyPct);
    KVIndexConfigBuilder& SetEnableCompactHashKey(bool flag);
    KVIndexConfigBuilder& SetEnableShortenOffset(bool flag);
    KVIndexConfigBuilder& SetMaxValueSizeForShortOffset(size_t size);
    KVIndexConfigBuilder& SetValueFormat(const std::string& valueFormat);
    KVIndexConfigBuilder& SetValueCompressionParameters(const std::string& compressorType,
                                                        const std::map<std::string, std::string>& params = {});
    KVIndexConfigBuilder& SetEnableFixLenAutoInline(bool flag);

    using SchemaType = std::shared_ptr<indexlibv2::config::TabletSchema>;
    using IndexConfigType = std::shared_ptr<indexlibv2::config::KVIndexConfig>;
    using Result = std::pair<SchemaType, IndexConfigType>;
    Result Finalize() { return {std::move(_schema), std::move(_indexConfig)}; }

public:
    static Result MakeIndexConfig(const std::string& fields, const std::string& key, const std::string& values,
                                  int64_t ttl = -1);

private:
    SchemaType _schema;
    IndexConfigType _indexConfig;
};

} // namespace indexlibv2::index

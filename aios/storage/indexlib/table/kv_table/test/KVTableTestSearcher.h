#pragma once

#include "autil/Log.h"
#include "indexlib/index/kv/Types.h"

namespace indexlibv2::config {
class KVIndexConfig;
class ValueConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {
class FieldValueExtractor;
class PackAttributeFormatter;
class KVIndexReader;
} // namespace indexlibv2::index

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::table {
class Result;

class KVTableTestSearcher
{
public:
    KVTableTestSearcher();
    ~KVTableTestSearcher();

public:
    void Init(std::shared_ptr<indexlibv2::index::KVIndexReader> reader,
              std::shared_ptr<indexlibv2::config::KVIndexConfig> kvIndexConfig);

    std::shared_ptr<Result> Search(const std::string& key, uint64_t timestamp,
                                   indexlib::TableSearchCacheType searchCacheType);

private:
    void FillResult(const std::shared_ptr<Result>& result,
                    const indexlibv2::index::FieldValueExtractor& valueExtractor);

private:
    std::shared_ptr<indexlibv2::index::KVIndexReader> _kvReader;
    std::shared_ptr<indexlibv2::config::ValueConfig> _valueConfig;
    std::shared_ptr<indexlibv2::index::PackAttributeFormatter> _packAttrFormatter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table

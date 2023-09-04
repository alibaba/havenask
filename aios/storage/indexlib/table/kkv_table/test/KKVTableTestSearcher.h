#pragma once

#include <vector>
#include "autil/Log.h"
#include "future_lite/executors/SimpleExecutor.h"
#include "indexlib/table/kkv_table/KKVReader.h"
#include "indexlib/table/test/Result.h"

namespace indexlibv2::config {
class KKVIndexConfig;
class ValueConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {
class FieldValueExtractor;
class PackAttributeFormatter;
} // namespace indexlibv2::index

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::table {
class KKVReader;
class Result;

class KKVTableTestSearcher
{
public:
    KKVTableTestSearcher();
    ~KKVTableTestSearcher();

public:
    void Init(const std::shared_ptr<indexlibv2::table::KKVReader>& reader,
              const std::shared_ptr<indexlibv2::config::KKVIndexConfig>& indexConfig);

    std::shared_ptr<Result> Search(const std::string& pkeyStr, const std::vector<std::string>& skeyStrs,
                                   indexlibv2::table::KKVReadOptions& readOptions);

    std::shared_ptr<Result> Search(const std::vector<std::string>& pkeyStr,
                                   const std::vector<std::vector<std::string>>& skeyStrs,
                                   indexlibv2::table::KKVReadOptions& readOptions,
                                   bool batchFinish);

private:
    void FillResult(const std::shared_ptr<Result>& result, index::KKVIterator* iter,
                    indexlibv2::table::KKVReadOptions& readOptions);
    std::string GetSKeyValueStr(uint64_t skey);

private:
    future_lite::executors::SimpleExecutor _executor = {1};
    std::shared_ptr<indexlibv2::table::KKVReader> _kkvReader;
    std::shared_ptr<indexlibv2::config::KKVIndexConfig> _indexConfig;
    std::shared_ptr<indexlibv2::config::ValueConfig> _valueConfig;
    std::shared_ptr<indexlibv2::index::PackAttributeFormatter> _packAttrFormatter;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table

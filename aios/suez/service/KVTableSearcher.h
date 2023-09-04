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
#pragma once

#include "absl/container/flat_hash_map.h"
#include "autil/StringView.h"
#include "autil/result/Errors.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/KVMetricsCollector.h"
#include "suez/sdk/TableReader.h"

namespace indexlibv2 {
namespace table {
class KVTabletSessionReader;
} // namespace table
} // namespace indexlibv2

namespace indexlibv2 {
namespace framework {
class ITabletReader;
}
} // namespace indexlibv2

namespace indexlibv2 {
namespace index {
struct KVReadOptions;
}
} // namespace indexlibv2

namespace kmonitor {
class MetricsReporter;
}

namespace table {
class Table;
}

namespace suez {
struct KVTableSearcherMetricsCollector {
    indexlibv2::index::KVMetricsCollector indexCollector;
    size_t docsCount = 0;
    size_t failedDocsCount = 0;
    size_t notFoundDocsCount = 0;
    int64_t batchScanTime = 0;
    int64_t convertTime = 0;
    int64_t lookupTime = 0;
};

struct LookupOptions {
    std::vector<std::string> attrs;
    uint32_t timeout;
    future_lite::Executor *executor;
    autil::mem_pool::Pool *pool;

    LookupOptions(const std::vector<std::string> &attrs_,
                  uint32_t timeout_,
                  future_lite::Executor *executor_,
                  autil::mem_pool::Pool *pool_)
        : attrs(attrs_), timeout(timeout_), executor(executor_), pool(pool_) {}
};

struct LookupResult {
    absl::flat_hash_map<uint64_t, autil::StringView> foundValues;
    int32_t failedCount = 0;
    int32_t notFoundCount = 0;
};

class KVTableSearcher {
public:
    typedef std::vector<std::pair<int, std::vector<std::string>>> InputKeys;
    typedef std::vector<std::pair<int, std::vector<uint64_t>>> InputHashKeys;

public:
    KVTableSearcher(const MultiTableReader *multiTableReader);

    ~KVTableSearcher();
    autil::Result<> init(const std::string &tableName, const std::string &indexName);

public:
    autil::result::Result<LookupResult> lookup(const InputHashKeys &inputHashKeys, LookupOptions *options);

    // debug
    autil::result::Result<std::vector<std::string>> query(const InputKeys &inputKeys, LookupOptions *options);

    autil::result::Result<std::vector<uint64_t>> genHashKeys(size_t partId, const std::vector<std::string> &keys);

    void reportMetrics(kmonitor::MetricsReporter *searchMetricsReporter) const;

    autil::Result<std::string>
    convertResult(size_t partId, const std::vector<autil::StringView> &values, LookupOptions *lookupOpts);

private:
    std::string constructJsonQuery(const std::vector<std::string> &inputKeys, const std::vector<std::string> &attrs);
    indexlibv2::index::KVReadOptions createKvOptions(LookupOptions *lookupOpts);

private:
    std::map<int32_t, std::shared_ptr<indexlibv2::framework::ITabletReader>> _tabletReaderHolder;
    const MultiTableReader *_multiTableReader;
    std::string _tableName;
    std::string _indexName;
    KVTableSearcherMetricsCollector _metricsCollector;
};
} // namespace suez

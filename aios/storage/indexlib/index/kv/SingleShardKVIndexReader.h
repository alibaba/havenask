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

#include <memory>

#include "autil/Log.h"
#include "autil/TimeoutTerminator.h"
#include "indexlib/base/Constant.h"
#include "indexlib/base/Types.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/IIndexReader.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVIndexReader.h"

namespace indexlibv2::index {
class AdapterIgnoreFieldCalculator;

class SingleShardKVIndexReader : public KVIndexReader
{
public:
    SingleShardKVIndexReader(schemaid_t readerSchemaId);

public:
    Status DoOpen(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                  const framework::TabletData* tabletData) noexcept override;
    void ResetCounter(index::KVMetricsCollector* metricsCollector) const;

protected:
    FL_LAZY(KVResultStatus)
    InnerGet(const KVReadOptions* readOptions, index::keytype_t key, autil::StringView& value,
             index::KVMetricsCollector* metricsCollector = NULL) const noexcept override;

    virtual FL_LAZY(KVResultStatus)
        DoGet(const KVReadOptions* readOptions, index::keytype_t key, autil::StringView& value, uint64_t& valueTs,
              index::KVMetricsCollector* metricsCollector = NULL) const noexcept;

    FL_LAZY(KVResultStatus)
    GetFromSegmentReader(const std::shared_ptr<index::IKVSegmentReader>& segmentReader, index::keytype_t key,
                         autil::StringView& value, uint64_t& valueTs, autil::mem_pool::Pool* pool,
                         index::KVMetricsCollector* metricsCollector,
                         autil::TimeoutTerminator* timeoutTerminator) const noexcept;

private:
    Status LoadSegments(const std::shared_ptr<config::KVIndexConfig>& indexConfig,
                        const std::shared_ptr<AdapterIgnoreFieldCalculator>& ignoreFieldCalculator,
                        const framework::TabletData* tabletData, framework::Segment::SegmentStatus status);

    template <typename Iterator, bool inMemory>
    Status LoadSlice(const std::shared_ptr<config::KVIndexConfig>& indexConfig,
                     const std::shared_ptr<AdapterIgnoreFieldCalculator>& ignoreFieldCalculator, Iterator begin,
                     Iterator end);

private:
    bool _hasTTL;
    bool _kvReportMetrics;
    std::vector<std::shared_ptr<IKVSegmentReader>> _memorySegmentReaders;
    std::vector<std::shared_ptr<IKVSegmentReader>> _diskSegmentReaders;
    std::vector<std::shared_ptr<framework::Locator>> _diskSegmentLocators;

private:
    AUTIL_LOG_DECLARE();
};

inline void SingleShardKVIndexReader::ResetCounter(index::KVMetricsCollector* metricsCollector) const
{
    if (!_kvReportMetrics || !metricsCollector) {
        return;
    }
    metricsCollector->BeginMemTableQuery();
    metricsCollector->ResetBlockCounter();
}

inline FL_LAZY(KVResultStatus)
    SingleShardKVIndexReader::InnerGet(const KVReadOptions* readOptions, index::keytype_t key, autil::StringView& value,
                                       index::KVMetricsCollector* metricsCollector) const noexcept
{
    uint64_t valueTs = 0;
    uint64_t minimumTsInSecond = 0;
    uint64_t currentTimeInSecond = autil::TimeUtility::us2sec(readOptions->timestamp);
    if (currentTimeInSecond > _ttl) {
        minimumTsInSecond = currentTimeInSecond - _ttl;
    }
    auto status = FL_COAWAIT DoGet(readOptions, key, value, valueTs, metricsCollector);
    if (status == KVResultStatus::FOUND && _hasTTL) {
        status = valueTs >= minimumTsInSecond ? KVResultStatus::FOUND : KVResultStatus::NOT_FOUND;
    }
    if (_kvReportMetrics && metricsCollector) {
        metricsCollector->EndQuery();
    }
    FL_CORETURN status;
}

inline FL_LAZY(KVResultStatus)
    SingleShardKVIndexReader::DoGet(const KVReadOptions* readOptions, index::keytype_t key, autil::StringView& value,
                                    uint64_t& ts, index::KVMetricsCollector* metricsCollector) const noexcept
{
    ResetCounter(metricsCollector);

    if (_memorySegmentReaders.empty() && _diskSegmentReaders.empty()) {
        FL_CORETURN TranslateStatus(indexlib::util::NOT_FOUND);
    }

    auto pool = readOptions->pool;
    auto timeoutTerminator = readOptions->timeoutTerminator.get();

    for (const auto& reader : _memorySegmentReaders) {
        auto status =
            FL_COAWAIT GetFromSegmentReader(reader, key, value, ts, pool, metricsCollector, timeoutTerminator);
        if (status != KVResultStatus::NOT_FOUND) {
            FL_CORETURN status;
        }
    }
    if (_kvReportMetrics && metricsCollector) {
        metricsCollector->BeginSSTableQuery();
    }

    for (const auto& reader : _diskSegmentReaders) {
        auto status =
            FL_COAWAIT GetFromSegmentReader(reader, key, value, ts, pool, metricsCollector, timeoutTerminator);
        if (status != KVResultStatus::NOT_FOUND) {
            FL_CORETURN status;
        }
    }
    FL_CORETURN TranslateStatus(indexlib::util::NOT_FOUND);
}

inline FL_LAZY(KVResultStatus) SingleShardKVIndexReader::GetFromSegmentReader(
    const std::shared_ptr<index::IKVSegmentReader>& segmentReader, index::keytype_t key, autil::StringView& value,
    uint64_t& valueTs, autil::mem_pool::Pool* pool, index::KVMetricsCollector* metricsCollector,
    autil::TimeoutTerminator* timeoutTerminator) const noexcept
{
    if (timeoutTerminator && timeoutTerminator->checkRestrictTimeout()) {
        FL_CORETURN KVResultStatus::TIMEOUT;
    }
    indexlib::util::Status status;
    try {
        status = FL_COAWAIT segmentReader->Get(key, value, valueTs, pool, metricsCollector, timeoutTerminator);
        if (status == indexlib::util::OK || status == indexlib::util::DELETED) {
            if (_kvReportMetrics && metricsCollector) {
                metricsCollector->IncResultCount();
            }
        }
    } catch (const std::exception& e) {
        AUTIL_LOG(ERROR, "should not throw exception, [%s]", e.what());
        FL_CORETURN KVResultStatus::FAIL;
    } catch (...) {
        AUTIL_LOG(ERROR, "should not throw exception");
        FL_CORETURN KVResultStatus::FAIL;
    }

    FL_CORETURN TranslateStatus(status);
}

} // namespace indexlibv2::index

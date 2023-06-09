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

#include <vector>

#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/TimeoutTerminator.h"
#include "indexlib/framework/LevelInfo.h"
#include "indexlib/framework/TabletData.h"
#include "indexlib/index/kv/IKVSegmentReader.h"
#include "indexlib/index/kv/KVIndexReader.h"
#include "indexlib/index/kv/KVMetricsCollector.h"
#include "indexlib/util/ShardUtil.h"
#include "indexlib/util/Status.h"

namespace indexlibv2::index {
class AdapterIgnoreFieldCalculator;
}

namespace indexlibv2::table {

class KVReaderImpl : public index::KVIndexReader
{
public:
    typedef std::vector<
        std::vector<std::pair<std::shared_ptr<index::IKVSegmentReader>, std::shared_ptr<framework::Locator>>>>
        SegmentShardReaderVector;

public:
    KVReaderImpl(schemaid_t readerSchemaId) : index::KVIndexReader(readerSchemaId)
    {
        _hasTTL = false;
        _kvReportMetrics = true;
    }

protected:
    Status DoOpen(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                  const framework::TabletData* tabletData) noexcept override;
    void ResetCounter(index::KVMetricsCollector* metricsCollector) const;

    FL_LAZY(index::KVResultStatus)
    InnerGet(const index::KVReadOptions* readOptions, index::keytype_t key, autil::StringView& value,
             index::KVMetricsCollector* metricsCollector = NULL) const noexcept override;

    virtual FL_LAZY(index::KVResultStatus)
        DoGet(const index::KVReadOptions* readOptions, index::keytype_t key, autil::StringView& value,
              uint64_t& valueTs, index::KVMetricsCollector* metricsCollector = NULL) const noexcept;

    FL_LAZY(index::KVResultStatus)
    GetFromMemSegment(index::keytype_t key, autil::StringView& value, uint64_t& valueTs, size_t shardId,
                      autil::mem_pool::Pool* pool, index::KVMetricsCollector* metricsCollector,
                      autil::TimeoutTerminator* timeoutTerminator) const noexcept;

    FL_LAZY(index::KVResultStatus)
    GetFromDiskSegments(const std::shared_ptr<framework::Locator>& locator, index::keytype_t key,
                        autil::StringView& value, uint64_t& valueTs, size_t shardId, autil::mem_pool::Pool* pool,
                        index::KVMetricsCollector* metricsCollector,
                        autil::TimeoutTerminator* timeoutTerminator) const noexcept;

    FL_LAZY(index::KVResultStatus)
    GetFromSegmentReader(const std::shared_ptr<index::IKVSegmentReader>& segmentReader, index::keytype_t key,
                         autil::StringView& value, uint64_t& valueTs, autil::mem_pool::Pool* pool,
                         index::KVMetricsCollector* metricsCollector,
                         autil::TimeoutTerminator* timeoutTerminator) const noexcept;

    size_t GetShardId(index::keytype_t key) const;

private:
    Status LoadSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                             const framework::TabletData* tabletData) noexcept;

    Status LoadMemSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                const framework::TabletData* tabletData) noexcept;

    Status LoadDiskSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                 const framework::TabletData* tabletData,
                                 const std::shared_ptr<framework::LevelInfo>& levelInfo, size_t shardCount) noexcept;

    Status AddMemSegments(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                          framework::TabletData::Slice& slice) noexcept;
    Status LoadMemSegmentReader(const std::shared_ptr<indexlibv2::config::KVIndexConfig>& kvIndexConfig,
                                framework::Segment* segment) noexcept;

public:
    SegmentShardReaderVector& TEST_GetMemoryShardReaders();
    SegmentShardReaderVector& TEST_GetDiskShardReaders();

protected:
    bool _hasTTL;
    bool _kvReportMetrics;
    SegmentShardReaderVector _memoryShardReaders;
    SegmentShardReaderVector _diskShardReaders;
    std::shared_ptr<index::AdapterIgnoreFieldCalculator> _ignoreFieldCalculator;

private:
    AUTIL_LOG_DECLARE();
};

///////////////////////////////////////////////////////////////////////
inline void KVReaderImpl::ResetCounter(index::KVMetricsCollector* metricsCollector) const
{
    if (!_kvReportMetrics || !metricsCollector) {
        return;
    }
    metricsCollector->BeginMemTableQuery();
    metricsCollector->ResetBlockCounter();
}

inline FL_LAZY(index::KVResultStatus) KVReaderImpl::InnerGet(const index::KVReadOptions* readOptions,
                                                             index::keytype_t key, autil::StringView& value,
                                                             index::KVMetricsCollector* metricsCollector) const noexcept
{
    uint64_t valueTs = 0;
    uint64_t minimumTsInSecond = 0;
    uint64_t currentTimeInSecond = autil::TimeUtility::us2sec(readOptions->timestamp);
    if (currentTimeInSecond > _ttl) {
        minimumTsInSecond = currentTimeInSecond - _ttl;
    }
    auto status = FL_COAWAIT DoGet(readOptions, key, value, valueTs, metricsCollector);
    if (status == index::KVResultStatus::FOUND && _hasTTL) {
        status = valueTs >= minimumTsInSecond ? index::KVResultStatus::FOUND : index::KVResultStatus::NOT_FOUND;
    }
    if (_kvReportMetrics && metricsCollector) {
        metricsCollector->EndQuery();
    }
    FL_CORETURN status;
}

inline FL_LAZY(index::KVResultStatus) KVReaderImpl::DoGet(const index::KVReadOptions* readOptions, index::keytype_t key,
                                                          autil::StringView& value, uint64_t& valueTs,
                                                          index::KVMetricsCollector* metricsCollector) const noexcept
{
    ResetCounter(metricsCollector);
    if (_diskShardReaders.empty() && _memoryShardReaders.empty()) {
        FL_CORETURN index::KVResultStatus::NOT_FOUND;
    }
    auto pool = readOptions->pool;
    auto timeoutTerminator = readOptions->timeoutTerminator.get();
    auto shardId = GetShardId(key);
    auto status = FL_COAWAIT GetFromMemSegment(key, value, valueTs, shardId, pool, metricsCollector, timeoutTerminator);
    if (status != index::KVResultStatus::NOT_FOUND) {
        FL_CORETURN status;
    }

    if (_kvReportMetrics && metricsCollector) {
        metricsCollector->BeginSSTableQuery();
    }
    FL_CORETURN FL_COAWAIT GetFromDiskSegments(nullptr, key, value, valueTs, shardId, pool, metricsCollector,
                                               timeoutTerminator);
}

inline FL_LAZY(index::KVResultStatus) KVReaderImpl::GetFromMemSegment(
    index::keytype_t key, autil::StringView& value, uint64_t& valueTs, size_t shardId, autil::mem_pool::Pool* pool,
    index::KVMetricsCollector* metricsCollector, autil::TimeoutTerminator* timeoutTerminator) const noexcept
{
    if (_memoryShardReaders.empty()) {
        FL_CORETURN index::KVResultStatus::NOT_FOUND;
    }
    for (size_t i = 0; i < _memoryShardReaders[shardId].size(); ++i) {
        auto& segmentReader = _memoryShardReaders[shardId][i].first;
        auto status = FL_COAWAIT GetFromSegmentReader(segmentReader, key, value, valueTs, pool, metricsCollector,
                                                      timeoutTerminator);
        if (status == index::KVResultStatus::NOT_FOUND) {
            continue;
        } else {
            // fail, delete, found
            FL_CORETURN status;
        }
    }
    FL_CORETURN index::KVResultStatus::NOT_FOUND;
}

inline FL_LAZY(index::KVResultStatus) KVReaderImpl::GetFromDiskSegments(
    const std::shared_ptr<framework::Locator>& locator, index::keytype_t key, autil::StringView& value,
    uint64_t& valueTs, size_t shardId, autil::mem_pool::Pool* pool, index::KVMetricsCollector* metricsCollector,
    autil::TimeoutTerminator* timeoutTerminator) const noexcept
{
    if (_diskShardReaders.empty()) {
        FL_CORETURN index::KVResultStatus::NOT_FOUND;
    }
    index::KVResultStatus status = index::KVResultStatus::NOT_FOUND;
    for (size_t i = 0; i < _diskShardReaders[shardId].size(); ++i) {
        auto& segmentReader = _diskShardReaders[shardId][i].first;
        const auto& segmentLocator = _diskShardReaders[shardId][i].second;
        if (nullptr != locator && framework::Locator::LocatorCompareResult::LCR_FULLY_FASTER ==
                                      locator->IsFasterThan(*segmentLocator, true)) {
            break;
        }
        status = FL_COAWAIT GetFromSegmentReader(segmentReader, key, value, valueTs, pool, metricsCollector,
                                                 timeoutTerminator);
        if (status == index::KVResultStatus::NOT_FOUND) {
            continue;
        } else {
            // fail, delete, found
            break;
        }
    }
    FL_CORETURN status;
}

inline FL_LAZY(index::KVResultStatus) KVReaderImpl::GetFromSegmentReader(
    const std::shared_ptr<index::IKVSegmentReader>& segmentReader, index::keytype_t key, autil::StringView& value,
    uint64_t& valueTs, autil::mem_pool::Pool* pool, index::KVMetricsCollector* metricsCollector,
    autil::TimeoutTerminator* timeoutTerminator) const noexcept
{
    if (timeoutTerminator && timeoutTerminator->checkRestrictTimeout()) {
        FL_CORETURN index::KVResultStatus::TIMEOUT;
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
        FL_CORETURN index::KVResultStatus::FAIL;
    } catch (...) {
        AUTIL_LOG(ERROR, "should not throw exception");
        FL_CORETURN index::KVResultStatus::FAIL;
    }

    FL_CORETURN index::TranslateStatus(status);
}

inline size_t KVReaderImpl::GetShardId(index::keytype_t key) const
{
    size_t shardCount = _memoryShardReaders.empty() ? _diskShardReaders.size() : _memoryShardReaders.size();
    auto shardId = indexlib::util::ShardUtil::GetShardId(key, shardCount);
    return shardId;
}

} // namespace indexlibv2::table

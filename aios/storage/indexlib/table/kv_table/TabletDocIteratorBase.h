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

#include "autil/Log.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/base/Status.h"
#include "indexlib/framework/ITabletDocIterator.h"
#include "indexlib/index/IShardRecordIterator.h"

namespace indexlibv2::config {
class TabletSchema;
class ValueConfig;
} // namespace indexlibv2::config
namespace indexlibv2::document {
class RawDocument;
}

namespace indexlibv2::framework {
class TabletData;
} // namespace indexlibv2::framework

namespace indexlibv2::index {
class PlainFormatEncoder;
class PackAttributeFormatter;
class AdapterIgnoreFieldCalculator;
} // namespace indexlibv2::index

namespace indexlibv2::table {

class TabletDocIteratorBase : public framework::ITabletDocIterator
{
public:
    TabletDocIteratorBase() = default;
    ~TabletDocIteratorBase() = default;

public:
    struct TabletCheckpoint {
        size_t shardIterIdx;
        offset_t segmentIterIdx;
        offset_t segmentOffset;
    };

public:
    Status Init(const std::shared_ptr<framework::TabletData>& tabletData,
                std::pair<uint32_t /*0-100*/, uint32_t /*0-100*/> rangeInRatio,
                const std::shared_ptr<indexlibv2::framework::MetricsManager>&,
                const std::map<std::string, std::string>& params) override;
    Status Next(indexlibv2::document::RawDocument* rawDocument, std::string* checkpoint,
                document::IDocument::DocInfo* docInfo) override;
    bool HasNext() const override;
    Status Seek(const std::string& checkpoint) override;
    void SetCheckPoint(const std::string& shardCheckpoint, std::string* checkpoint);

protected:
    Status DoInit(const std::shared_ptr<framework::TabletData>& tabletData,
                  const std::map<std::string, std::string>& params);
    Status InitFormatter(const std::shared_ptr<config::ValueConfig>& valueConfig,
                         const std::shared_ptr<config::ValueConfig>& pkValueConfig);

    Status SelectTargetShards(size_t& shardCount, std::pair<uint32_t /*0-100*/, uint32_t /*0-100*/> rangeInRatio);
    Status InitIterators(const std::shared_ptr<framework::TabletData>& tabletData,
                         const std::map<std::string, std::string>& params);

    Status ReadValue(const index::IShardRecordIterator::ShardRecord* shardRecord, size_t recordShardId,
                     indexlibv2::document::RawDocument* doc);

    bool NotInFields(const std::string& fieldName) const;
    Status MoveToNext();
    const std::vector<std::unique_ptr<index::IShardRecordIterator>>& TEST_GetShardIterators()
    {
        return _shardDocIterators;
    }

    virtual Status InitFromTabletData(const std::shared_ptr<framework::TabletData>& tabletData) = 0;
    virtual std::unique_ptr<index::IShardRecordIterator> CreateShardDocIterator() const = 0;
    virtual autil::StringView PreprocessPackValue(const autil::StringView& value) = 0;

protected:
    static const size_t MAX_RESET_POOL_MEMORY_THRESHOLD = 5 * 1024 * 1024;
    static const size_t MAX_RELEASE_POOL_MEMORY_THRESHOLD = 10 * 1024 * 1024;
    static const uint32_t RANGE_UPPER_BOUND = 100;
    static const std::string USER_REQUIRED_FIELDS;
    static const std::string READER_TIMESTAMP;
    int64_t _timeStamp = 0;
    size_t _shardIterId = 0;
    autil::mem_pool::Pool _pool;
    std::string _nextShardCheckpoint;
    index::IShardRecordIterator::ShardRecord _nextRecord;
    std::vector<std::string> _fieldNames;
    std::pair<uint32_t, uint32_t> _targetShardRange;
    std::shared_ptr<config::ValueConfig> _valueConfig;
    std::shared_ptr<config::TabletSchema> _tabletSchema;
    std::shared_ptr<index::PackAttributeFormatter> _formatter;
    std::shared_ptr<index::PackAttributeFormatter> _pkFormatter;
    std::shared_ptr<index::PlainFormatEncoder> _plainFormatEncoder;
    std::shared_ptr<index::AdapterIgnoreFieldCalculator> _ignoreFieldCalculator;
    std::vector<std::unique_ptr<index::IShardRecordIterator>> _shardDocIterators;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table

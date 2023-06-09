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

#include "indexlib/base/Types.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"

namespace autil::mem_pool {
class Pool;
}
namespace indexlibv2::config {
class IIndexConfig;
class AttributeConfig;
} // namespace indexlibv2::config

namespace indexlibv2::index {
class DocMapper;
}

namespace indexlib::index {

class TruncateAttributeReader : public indexlibv2::index::AttributeDiskIndexer
{
public:
    TruncateAttributeReader(const std::shared_ptr<indexlibv2::index::DocMapper>& docMapper,
                            const std::shared_ptr<indexlibv2::config::AttributeConfig>& attrConfig)
        : _docMapper(docMapper)
    {
    }
    ~TruncateAttributeReader() = default;

    struct ReadContext : public indexlibv2::index::AttributeDiskIndexer::ReadContextBase {
        std::map<segmentid_t, std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>> segCtx;
    };

public:
    bool IsInMemory() const override
    {
        assert(false);
        return false;
    }
    bool Updatable() const override
    {
        assert(false);
        return false;
    }
    bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull, const uint64_t* hashKey) override
    {
        assert(false);
        return false;
    }
    uint32_t TEST_GetDataLength(docid_t docId, autil::mem_pool::Pool* pool) const override
    {
        assert(false);
        return 0;
    }
    std::pair<Status, uint64_t>
    GetOffset(docid_t docId,
              const std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>& ctx) const override
    {
        assert(false);
        return {Status::Corruption("not support"), 0};
    }

    bool Read(docid_t docId, std::string* value, autil::mem_pool::Pool* pool) override
    {
        assert(false);
        return false;
    }
    size_t EvaluateCurrentMemUsed() override
    {
        assert(false);
        return 0;
    }
    Status Open(const std::shared_ptr<indexlibv2::config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override
    {
        assert(false);
        return Status::Corruption("not support");
    }
    bool Read(docid_t docId, const std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>& ctx,
              uint8_t* buf, uint32_t bufLen, uint32_t& dataLen, bool& isNull) override;
    void AddAttributeDiskIndexer(segmentid_t segId,
                                 const std::shared_ptr<indexlibv2::index::AttributeDiskIndexer>& segmentReader);
    std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>
    CreateReadContextPtr(autil::mem_pool::Pool* pool) const override;

private:
    std::shared_ptr<indexlibv2::index::DocMapper> _docMapper;
    std::map<segmentid_t, std::shared_ptr<indexlibv2::index::AttributeDiskIndexer>> _diskIndexers;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index

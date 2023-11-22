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
#include "autil/NoCopyable.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/AttributeDiskIndexerCreator.h"

namespace indexlibv2::index {

class MultiSliceAttributeDiskIndexer : public AttributeDiskIndexer
{
public:
    MultiSliceAttributeDiskIndexer(std::shared_ptr<AttributeMetrics> attributeMetrics,
                                   const DiskIndexerParameter& indexerParam, AttributeDiskIndexerCreator* creator)
        : AttributeDiskIndexer(attributeMetrics, indexerParam)
        , _creator(creator)
    {
    }
    ~MultiSliceAttributeDiskIndexer() {}
    struct MultiSliceReadContext : public ReadContextBase {
        std::vector<std::shared_ptr<ReadContextBase>> sliceReadContexts;
    };

public:
    void EnableGlobalReadContext() override;
    Status Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    bool IsInMemory() const override { return false; }
    bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull, const uint64_t* hashKey) override;

    bool Updatable() const override
    {
        assert(false);
        return false;
    }

    std::pair<Status, uint64_t> GetOffset(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx) const override;
    std::shared_ptr<ReadContextBase> CreateReadContextPtr(autil::mem_pool::Pool* pool) const override;
    bool Read(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx, uint8_t* buf, uint32_t bufLen,
              uint32_t& dataLen, bool& isNull) override;
    Status SetPatchReader(const std::shared_ptr<AttributePatchReader>& patchReader, docid_t patchBaseDocId) override;

    int32_t GetSliceCount() const { return _sliceAttributes.size(); }

    template <typename DiskIndexer>
    std::shared_ptr<DiskIndexer> GetSliceIndexer(int32_t idx)
    {
        if (idx >= _sliceAttributes.size()) {
            return nullptr;
        }
        return std::dynamic_pointer_cast<DiskIndexer>(_sliceAttributes[idx]);
    }

    int64_t GetSliceDocCount(int32_t idx) const { return _sliceDocCounts[idx]; }
    bool Read(docid_t docId, std::string* value, autil::mem_pool::Pool* pool) override;
    bool ReadBinaryValue(docid_t docId, autil::StringView* value, autil::mem_pool::Pool* pool) override;

    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory) override;
    size_t EvaluateCurrentMemUsed() override;

public:
    // for test
    uint32_t TEST_GetDataLength(docid_t docId, autil::mem_pool::Pool* pool) const override;
    MultiSliceAttributeDiskIndexer(std::shared_ptr<AttributeDiskIndexer> indexer, int64_t docCount)
    {
        _sliceAttributes.push_back(indexer);
        _sliceBaseDocIds.push_back(0);
        _sliceDocCounts.push_back(docCount);
    }

private:
    Status AddSliceIndexer(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& attrDirectory, docid_t baseDocId,
                           int64_t docCount);
    std::shared_ptr<AttributeDiskIndexer::ReadContextBase>
    GetSliceReadContextPtr(const std::shared_ptr<AttributeDiskIndexer::ReadContextBase>& ctx, size_t sliceIdx) const;
    int64_t GetSliceIdxByDocId(docid_t docId) const;

private:
    AttributeDiskIndexerCreator* _creator;
    std::vector<std::shared_ptr<AttributeDiskIndexer>> _sliceAttributes;
    std::vector<docid_t> _sliceBaseDocIds;
    std::vector<int64_t> _sliceDocCounts;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index

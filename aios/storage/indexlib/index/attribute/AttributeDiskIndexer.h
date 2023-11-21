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
#include "indexlib/index/DiskIndexerParameter.h"
#include "indexlib/index/IDiskIndexer.h"
#include "indexlib/index/attribute/AttributeDataInfo.h"
#include "indexlib/index/attribute/AttributeMetrics.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/common/field_format/attribute/AttributeFieldPrinter.h"
#include "indexlib/index/common/patch/PatchFileInfo.h"

namespace autil::mem_pool {
class Pool;
}
namespace indexlibv2::framework {
class IIndexMemoryReclaimer;
} // namespace indexlibv2::framework

namespace indexlibv2::index {

class AttributePatchReader;
class IAttributePatch;

class AttributeDiskIndexer : public IDiskIndexer
{
public:
    AttributeDiskIndexer() : _attributeMetrics(nullptr) {}
    AttributeDiskIndexer(std::shared_ptr<AttributeMetrics> attributeMetrics, const DiskIndexerParameter& indexerParam)
        : _attributeMetrics(attributeMetrics)
        , _indexerParam(indexerParam)
        , _globalCtxSwitchLimit(0)
        , _enableAccessCountors(false)
    {
    }
    virtual ~AttributeDiskIndexer() {}

public:
    struct ReadContextBase {
        ReadContextBase() = default;
        virtual ~ReadContextBase() {};
    };

public:
    size_t EstimateMemUsed(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                           const std::shared_ptr<indexlib::file_system::IDirectory>& attrDirectory) override;

public:
    virtual bool IsInMemory() const = 0;
    virtual bool UpdateField(docid_t docId, const autil::StringView& value, bool isNull, const uint64_t* hashKey) = 0;

    virtual bool Updatable() const = 0;
    void EnableAccessCountors() { _enableAccessCountors = true; }

    virtual std::pair<Status, uint64_t> GetOffset(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx) const = 0;
    virtual std::shared_ptr<ReadContextBase> CreateReadContextPtr(autil::mem_pool::Pool* pool) const = 0;
    virtual void EnableGlobalReadContext();
    ReadContextBase* GetGlobalReadContext()
    {
        if (_globalCtxPool && _globalCtxPool->getUsedBytes() > _globalCtxSwitchLimit) {
            _globalCtx.reset();
            _globalCtxPool->release();
            _globalCtx = CreateReadContextPtr(_globalCtxPool.get());
        }
        return _globalCtx.get();
    }
    virtual bool Read(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx, uint8_t* buf, uint32_t bufLen,
                      uint32_t& dataLen, bool& isNull) = 0;
    virtual Status SetPatchReader(const std::shared_ptr<AttributePatchReader>& patchReader, docid_t patchBaseDocId);
    virtual bool Read(docid_t docId, std::string* value, autil::mem_pool::Pool* pool) = 0;
    virtual bool ReadBinaryValue(docid_t docId, autil::StringView* value, autil::mem_pool::Pool* pool) = 0;

    virtual AttributeDataInfo GetAttributeDataInfo() const { return _dataInfo; }

public:
    virtual uint32_t TEST_GetDataLength(docid_t docId, autil::mem_pool::Pool* pool) const = 0;
    bool TEST_UpdateField(docid_t docId, const autil::StringView& value, bool isNull)
    {
        return UpdateField(docId, value, isNull, /*hashKey*/ nullptr);
    }
    bool TEST_UpdateField(docid_t docId, const autil::StringView& value, bool isNull, const uint64_t* hashKey)
    {
        return UpdateField(docId, value, isNull, hashKey);
    }
    std::shared_ptr<AttributeMetrics> TEST_GetAttributeMetrics() const { return _attributeMetrics; }

protected:
    virtual std::string GetAttributePath(const std::shared_ptr<AttributeConfig>& attrConfig);

protected:
    std::shared_ptr<AttributeMetrics> _attributeMetrics;
    std::shared_ptr<IAttributePatch> _patch;
    docid_t _patchBaseDocId = 0;
    DiskIndexerParameter _indexerParam;
    size_t _globalCtxSwitchLimit;
    bool _enableAccessCountors;
    std::shared_ptr<autil::mem_pool::Pool> _globalCtxPool;
    std::shared_ptr<ReadContextBase> _globalCtx;
    std::shared_ptr<AttributeConfig> _attrConfig;
    std::shared_ptr<AttributeFieldPrinter> _fieldPrinter;
    AttributeDataInfo _dataInfo;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index

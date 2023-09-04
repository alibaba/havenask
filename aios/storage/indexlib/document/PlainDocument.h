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
#include <map>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/document/IDocument.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/util/PooledUniquePtr.h"

namespace indexlib::util {
template <typename T>
class PooledUniquePtr;
}

namespace autil::mem_pool {
class Pool;
}

namespace indexlibv2::document {

class IIndexFields;
class IIndexFieldsParser;

// TODO: do not inherit from IDocument
class PlainDocument : public IDocument
{
    // format described as indexlib/document/PlainDocumentMessage.fbs
public:
    static constexpr uint32_t SERIALIZE_VERSION = 100000;

public:
    explicit PlainDocument(autil::mem_pool::Pool* pool);

    const framework::Locator& GetLocatorV2() const override;
    DocInfo GetDocInfo() const override;
    uint32_t GetTTL() const override;
    docid_t GetDocId() const override;
    DocOperateType GetDocOperateType() const override;
    bool HasFormatError() const override;
    autil::StringView GetTraceId() const override;
    int64_t GetIngestionTimestamp() const override;
    autil::StringView GetSource() const override;

public:
    void SetLocator(const framework::Locator& locator) override;
    void SetDocInfo(const DocInfo& docInfo) override;
    void SetDocOperateType(DocOperateType type) override;
    void SetTrace(bool trace) override;

public:
    size_t EstimateMemory() const override;
    void serialize(autil::DataBuffer& dataBuffer) const override;
    void deserialize(autil::DataBuffer& dataBuffer) override; // for test
    void deserialize(autil::DataBuffer& dataBuffer,
                     const std::unordered_map<autil::StringView, std::unique_ptr<const IIndexFieldsParser>>& parsers);

public:
    autil::mem_pool::Pool* GetPool() const { return _pool.get(); }
    schemaid_t GetSchemaId() const { return _schemaId; }

    [[nodiscard]] bool AddIndexFields(indexlib::util::PooledUniquePtr<IIndexFields> indexFields);
    IIndexFields* GetIndexFields(autil::StringView indexType);

    void SetIngestionTimestamp(int64_t timestamp);
    void SetSource(autil::StringView source);
    void SetTraceId(autil::StringView traceId);
    void SetTTL(uint32_t ttl);
    void SetSchemaId(schemaid_t schemaId) { _schemaId = schemaId; }

private:
    using IndexFieldsPtr = indexlib::util::PooledUniquePtr<IIndexFields>;
    using PoolPtr = std::unique_ptr<autil::mem_pool::Pool, std::function<void(autil::mem_pool::Pool*)>>;

private:
    // do not serialize/deserialize
    PoolPtr _pool; // destroy at last
    autil::StringView _fbBuffer;
    indexlibv2::framework::Locator _locator;

    // serialize/deserialize item
    std::unordered_map<autil::StringView, IndexFieldsPtr> _indexFieldsMap;
    DocOperateType _opType = ADD_DOC;
    uint32_t _ttl = UNINITIALIZED_TTL;
    DocInfo _docInfo;
    int64_t _ingestionTimestamp = INVALID_TIMESTAMP;
    autil::StringView _source;
    bool _trace = false;
    autil::StringView _traceId;
    schemaid_t _schemaId = DEFAULT_SCHEMAID;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::document

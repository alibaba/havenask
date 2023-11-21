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
#include "indexlib/document/PlainDocument.h"

#include <assert.h>
#include <flatbuffers/flatbuffers.h>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/ConstString.h"
#include "autil/DataBuffer.h"
#include "autil/StringView.h"
#include "indexlib/base/Status.h"
#include "indexlib/document/DocumentMemPoolFactory.h"
#include "indexlib/document/IIndexFields.h"
#include "indexlib/document/IIndexFieldsParser.h"
#include "indexlib/document/PlainDocumentMessage_generated.h"
#include "indexlib/index/IIndexFactory.h"
#include "indexlib/index/IndexFactoryCreator.h"
#include "indexlib/util/Exception.h"

namespace indexlibv2::document {
AUTIL_LOG_SETUP(indexlib.document, PlainDocument);

PlainDocument::PlainDocument()
{
    _recyclePool = indexlib::util::Singleton<DocumentMemPoolFactory>::GetInstance()->GetPool();
}

PlainDocument::~PlainDocument()
{
    _indexFieldsMap.clear();
    if (_recyclePool) {
        indexlib::util::Singleton<DocumentMemPoolFactory>::GetInstance()->Recycle(_recyclePool);
    }
}

const framework::Locator& PlainDocument::GetLocatorV2() const { return _locator; }

framework::Locator::DocInfo PlainDocument::GetDocInfo() const { return _docInfo; }

uint32_t PlainDocument::GetTTL() const { return _ttl; }

void PlainDocument::SetTTL(uint32_t ttl) { _ttl = ttl; }

docid_t PlainDocument::GetDocId() const
{
    assert(false);
    return INVALID_DOCID;
}

DocOperateType PlainDocument::GetDocOperateType() const { return _opType; }

bool PlainDocument::HasFormatError() const { return _hasFormatError; }

autil::StringView PlainDocument::GetTraceId() const
{
    if (!_trace) {
        return autil::StringView::empty_instance();
    }
    return _traceId;
}

int64_t PlainDocument::GetIngestionTimestamp() const { return _ingestionTimestamp; }
void PlainDocument::SetIngestionTimestamp(int64_t timestamp) { _ingestionTimestamp = timestamp; }

autil::StringView PlainDocument::GetSource() const { return _source; }
void PlainDocument::SetSource(autil::StringView source)
{
    _source = autil::MakeCString(source.data(), source.length(), GetPool());
}
void PlainDocument::SetTraceId(autil::StringView traceId)
{
    _traceId = autil::MakeCString(traceId.data(), traceId.length(), GetPool());
}

void PlainDocument::SetLocator(const framework::Locator& locator) { _locator = locator; }
void PlainDocument::SetDocInfo(const framework::Locator::DocInfo& docInfo) { _docInfo = docInfo; }
void PlainDocument::SetDocOperateType(DocOperateType type) { _opType = type; }
void PlainDocument::SetTrace(bool trace) { _trace = trace; }

size_t PlainDocument::EstimateMemory() const
{
    size_t memSize = sizeof(*this);
    memSize += GetPool()->getUsedBytes();
    for (auto& [_, indexFields] : _indexFieldsMap) {
        memSize += indexFields->EstimateMemory();
    }
    return memSize;
}

void PlainDocument::serialize(autil::DataBuffer& dataBuffer) const
{
    dataBuffer.write(SERIALIZE_VERSION);
    flatbuffers::FlatBufferBuilder fbb;
    std::vector<flatbuffers::Offset<indexlib::document::proto::IndexFieldsData>> dataOffsets;
    dataOffsets.reserve(_indexFieldsMap.size());
    autil::DataBuffer indexBuffer(autil::DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, GetPool());

    for (const auto& [indexType, indexFields] : _indexFieldsMap) {
        auto typeOffset = fbb.CreateString(indexType.data(), indexType.length());
        indexBuffer.clear();
        indexFields->serialize(indexBuffer);
        auto dataOffset = fbb.CreateString(indexBuffer.getData(), indexBuffer.getDataLen());

        dataOffsets.push_back(indexlib::document::proto::CreateIndexFieldsData(fbb, typeOffset, dataOffset));
    }
    auto sourceOffset = fbb.CreateString(_source.data(), _source.length());
    auto traceIdOffset = fbb.CreateString(_traceId.data(), _traceId.length());
    auto dataVectorOffset =
        fbb.CreateVector<flatbuffers::Offset<indexlib::document::proto::IndexFieldsData>>(dataOffsets);
    auto docOffset = indexlib::document::proto::CreatePlainDocumentMessage(
        fbb, _opType, _ttl, _docInfo.hashId, _docInfo.timestamp, _ingestionTimestamp, sourceOffset, _trace,
        traceIdOffset, dataVectorOffset, _schemaId);

    fbb.Finish(docOffset);

    uint32_t fbSize = fbb.GetSize();
    dataBuffer.write(fbSize);
    dataBuffer.writeBytes((char*)fbb.GetBufferPointer(), fbb.GetSize());
}

// for test
void PlainDocument::deserialize(autil::DataBuffer& dataBuffer)
{
    auto factoryCreator = index::IndexFactoryCreator::GetInstance();
    assert(factoryCreator);
    auto allFactories = factoryCreator->GetAllFactories();
    std::unordered_map<autil::StringView, std::unique_ptr<const IIndexFieldsParser>> parserMap;
    for (auto& [indexType, factory] : allFactories) {
        auto parser = factory->CreateIndexFieldsParser();
        assert(parser);
        [[maybe_unused]] auto status = parser->Init(/*indexConfigs*/ {}, /*documentInitParam*/ nullptr);
        assert(status.IsOK());
        parserMap[indexType] = std::move(parser);
    }
    deserialize(dataBuffer, parserMap);
}

void PlainDocument::deserialize(
    autil::DataBuffer& dataBuffer,
    const std::unordered_map<autil::StringView, std::unique_ptr<const IIndexFieldsParser>>& parsers)
{
    uint32_t serializeVersion = 0;
    dataBuffer.read(serializeVersion);
    assert(serializeVersion == SERIALIZE_VERSION);
    uint32_t fbSize = 0;
    dataBuffer.read(fbSize);
    _fbBuffer = autil::MakeCString(dataBuffer.getData(), fbSize, GetPool());
    dataBuffer.skipData(fbSize);

    auto msg = indexlib::document::proto::GetPlainDocumentMessage(_fbBuffer.data());

    _opType = (DocOperateType)msg->opType();
    _ttl = msg->ttl();
    _docInfo.hashId = msg->hashId();
    _docInfo.timestamp = msg->timestamp();
    _ingestionTimestamp = msg->ingestionTimestamp();
    auto sourceStr = msg->source();
    if (sourceStr) {
        _source = autil::StringView(sourceStr->data(), sourceStr->size());
    }
    _trace = msg->needTrace();
    auto traceIdStr = msg->traceId();
    if (traceIdStr) {
        _traceId = autil::StringView(traceIdStr->data(), traceIdStr->size());
    }
    _schemaId = msg->schemaId();
    auto indexFieldsVec = msg->indexFieldsVec();
    if (indexFieldsVec) {
        size_t count = indexFieldsVec->size();
        for (size_t i = 0; i < count; ++i) {
            const auto& indexDataFb = indexFieldsVec->Get(i);
            auto indexType = indexDataFb->type();
            auto iter = parsers.find(indexType->string_view());
            if (iter == parsers.end()) {
                INDEXLIB_FATAL_ERROR(DocumentDeserialize, "index type [%s] not registed ", indexType->c_str());
            }
            const auto& parser = iter->second;
            assert(parser);
            auto data = indexDataFb->data();
            auto indexFields = parser->Parse(autil::StringView(data->data(), data->size()), GetPool());
            if (!indexFields) {
                INDEXLIB_FATAL_ERROR(DocumentDeserialize, "create index field failed, index_type[%s]",
                                     indexType->c_str());
            }
            assert(indexType->str() == indexFields->GetIndexType().to_string());
            if (!AddIndexFields(std::move(indexFields))) {
                INDEXLIB_FATAL_ERROR(DocumentDeserialize, "add index document failed, type[%s]", indexType->c_str());
            }
        }
    }
}

bool PlainDocument::AddIndexFields(indexlib::util::PooledUniquePtr<IIndexFields> indexFields)
{
    auto ret = _indexFieldsMap.insert(std::make_pair(indexFields->GetIndexType(), std::move(indexFields)));
    if (!ret.second) {
        AUTIL_LOG(ERROR, "add index document [%s] failed", indexFields->GetIndexType().to_string().c_str());
        return false;
    }
    return true;
}

IIndexFields* PlainDocument::GetIndexFields(autil::StringView indexType)
{
    auto iter = _indexFieldsMap.find(indexType);
    if (iter == _indexFieldsMap.end()) {
        return nullptr;
    }
    return iter->second.get();
}

} // namespace indexlibv2::document

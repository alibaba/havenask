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

#include "autil/EnvUtil.h"
#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/file_system/stream/FileStream.h"
#include "indexlib/index/attribute/AttributeIteratorBase.h"
#include "indexlib/index/attribute/AttributeReaderTraits.h"
#include "indexlib/index/common/field_format/attribute/AttributeFieldPrinter.h"

namespace indexlibv2::index {

class MemoryFileStreamCreator
{
private:
    static std::shared_ptr<indexlib::file_system::FileStream>
    CreateMemoryFileStream(const autil::StringView& stringView);

    template <typename T, typename ReaderTraits>
    friend class UniqEncodeSequenceIteratorTyped;
};

template <typename T, typename ReaderTraits = AttributeReaderTraits<T>>
class UniqEncodeSequenceIteratorTyped : public AttributeIteratorBase
{
private:
    using AttributeDiskIndexerTyped = typename ReaderTraits::SegmentReader;

public:
    UniqEncodeSequenceIteratorTyped(const std::vector<std::shared_ptr<AttributeDiskIndexerTyped>>& diskIndexers,
                                    const std::vector<uint64_t>& segmentDocCounts, const AttributeFieldPrinter* printer)
        : AttributeIteratorBase(nullptr)
        , _diskIndexers(diskIndexers)
        , _printer(printer)

    {
        uint64_t baseDocId = 0;
        for (auto segmentDocCount : segmentDocCounts) {
            _segmentDocIds.push_back({baseDocId, baseDocId + segmentDocCount});
            baseDocId += segmentDocCount;
        }
        _globalCtxSwitchLimit = autil::EnvUtil::getEnv("GLOBAL_READ_CONTEXT_SWITCH_MEMORY_LIMIT", 2 * 1024 * 1024);
    }

    ~UniqEncodeSequenceIteratorTyped() = default;

public:
    void Reset() override
    {
        _currentSegmentIdx = -1;
        _currentReadContext.fileStream.reset();
        _uniqContentPool.release();
    }

    future_lite::coro::Lazy<indexlib::index::ErrorCodeVec> BatchSeek(const std::vector<docid_t>& docIds,
                                                                     indexlib::file_system::ReadOption readOption,
                                                                     std::vector<std::string>* values) noexcept override
    {
        assert(false);
        indexlib::index::ErrorCodeVec ret;
        co_return ret;
    }

    bool Seek(docid_t docId, std::string& attrValue) noexcept override
    {
        if (_tmpPool.getUsedBytes() > _globalCtxSwitchLimit) {
            auto currentFileStrem = _currentReadContext.fileStream;
            _currentReadContext = typename AttributeDiskIndexerTyped::ReadContext();
            _tmpPool.release();
            _currentReadContext = _diskIndexers[_currentSegmentIdx]->CreateReadContext(&_tmpPool);
            _currentReadContext.fileStream = currentFileStrem;
        }

        autil::MultiValueType<T> value;
        if (!Seek(docId, &value)) {
            return false;
        }

        return _printer->Print(value.isNull(), value, &attrValue);
    }

private:
    bool Seek(docid_t docId, autil::MultiValueType<T>* value) noexcept;

    std::pair<bool, std::shared_ptr<indexlib::file_system::FileStream>>
    CreateMemFileStream(const std::shared_ptr<indexlib::file_system::FileStream>& fileStream);

private:
    autil::mem_pool::Pool _tmpPool;
    autil::mem_pool::Pool _uniqContentPool;
    const std::vector<std::shared_ptr<AttributeDiskIndexerTyped>>& _diskIndexers;
    std::vector<std::pair<docid_t, docid_t>> _segmentDocIds;
    typename AttributeDiskIndexerTyped::ReadContext _currentReadContext;
    const AttributeFieldPrinter* _printer;
    int32_t _currentSegmentIdx = -1;
    size_t _globalCtxSwitchLimit = 0;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE_2(indexlib.index, UniqEncodeSequenceIteratorTyped, T, ReaderTraits);

template <typename T, typename ReaderTraits>
bool UniqEncodeSequenceIteratorTyped<T, ReaderTraits>::Seek(docid_t docId, autil::MultiValueType<T>* value) noexcept
{
    for (size_t i = std::max(0, _currentSegmentIdx); i < _segmentDocIds.size(); ++i) {
        auto [baseDocId, endDocId] = _segmentDocIds[i];
        if (docId >= baseDocId && docId < endDocId) {
            if (i != _currentSegmentIdx) {
                AUTIL_LOG(INFO, "uniq iterator seek switch segment idx [%zu] begin", i);
                _currentReadContext = _diskIndexers[i]->CreateReadContext(&_tmpPool);
                auto [ret, fileStream] = CreateMemFileStream(_currentReadContext.fileStream);
                if (!ret) {
                    return false;
                }
                _currentReadContext.fileStream = fileStream;
                _currentSegmentIdx = i;
                AUTIL_LOG(INFO, "uniq iterator seek switch segment idx [%zu] end", i);
            }
            bool isNull = false;
            if (!_diskIndexers[i]->Read(docId - baseDocId, *value, isNull, _currentReadContext)) {
                AUTIL_LOG(ERROR, "read doc [%d] failed", docId);
                return false;
            }
            return true;
        }
    }
    assert(false);
    return false;
}

template <typename T, typename ReaderTraits>
std::pair<bool, std::shared_ptr<indexlib::file_system::FileStream>>
UniqEncodeSequenceIteratorTyped<T, ReaderTraits>::CreateMemFileStream(
    const std::shared_ptr<indexlib::file_system::FileStream>& fileStream)
{
    _uniqContentPool.release();
    if (!fileStream) {
        return {true, nullptr};
    }
    auto fileLen = fileStream->GetStreamLength();
    void* data = _uniqContentPool.allocate(fileLen);
    auto [status, readLen] = fileStream->Read(data, fileLen, 0, indexlib::file_system::ReadOption()).StatusWith();
    if (!status.IsOK() || fileLen != readLen) {
        AUTIL_LOG(ERROR, "read file [%s] failed, file len is [%ld], read len [%ld]", fileStream->DebugString().c_str(),
                  fileLen, readLen);
        return {false, nullptr};
    }
    autil::StringView content((char*)data, readLen);
    return {true, MemoryFileStreamCreator::CreateMemoryFileStream(content)};
}

} // namespace indexlibv2::index

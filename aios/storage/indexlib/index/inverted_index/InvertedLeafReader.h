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
#include "future_lite/coro/Lazy.h"
#include "indexlib/file_system/file/ReadOption.h"
#include "indexlib/index/IndexerParameter.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/format/IndexFormatOption.h"

namespace indexlib::file_system {
class FileReader;
}

namespace indexlib::index {
class SegmentPosting;
class DictionaryReader;

class InvertedLeafReader : public IndexSegmentReader
{
public:
    InvertedLeafReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                       const IndexFormatOption& formatOption, uint64_t docCount,
                       const std::shared_ptr<DictionaryReader>& dictReader,
                       const std::shared_ptr<file_system::FileReader>& postingReader);
    ~InvertedLeafReader() = default;

    std::shared_ptr<DictionaryReader> GetDictionaryReader() const override { return _dictReader; }

    bool GetSegmentPosting(const index::DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool, file_system::ReadOption option,
                           InvertedIndexSearchTracer* tracer) const override;
    future_lite::coro::Lazy<index::Result<bool>>
    GetSegmentPostingAsync(const index::DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool, file_system::ReadOption option,
                           InvertedIndexSearchTracer* tracer) const noexcept;

    // update not support yet
    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete) override { assert(false); }

    uint64_t GetDocCount() const { return _docCount; }

    IndexFormatOption GetIndexFormatOption() const;
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> GetIndexConfig() const;
    std::shared_ptr<file_system::FileReader> GetPostingReader() const;

    size_t EvaluateCurrentMemUsed() const;

protected:
    void InnerGetSegmentPosting(dictvalue_t value, docid_t baseDocId, SegmentPosting& segPosting,
                                autil::mem_pool::Pool* sessionPool) const;
    future_lite::coro::Lazy<index::ErrorCode> GetSegmentPostingAsync(dictvalue_t value, docid_t baseDocId,
                                                                     SegmentPosting& segPosting,
                                                                     autil::mem_pool::Pool* sessionPool,
                                                                     file_system::ReadOption option,
                                                                     InvertedIndexSearchTracer* tracer) const noexcept;

private:
    std::shared_ptr<file_system::FileReader> GetSessionPostingFileReader(autil::mem_pool::Pool* sessionPool) const;

protected:
    std::shared_ptr<DictionaryReader> _dictReader;

private:
    std::shared_ptr<indexlibv2::config::InvertedIndexConfig> _indexConfig;
    IndexFormatOption _indexFormatOption;
    uint64_t _docCount = 0;
    std::shared_ptr<file_system::FileReader> _postingReader;
    uint8_t* _baseAddr = nullptr;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index

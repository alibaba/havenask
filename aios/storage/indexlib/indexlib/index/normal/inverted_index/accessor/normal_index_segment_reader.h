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

#include "future_lite/coro/Lazy.h"
#include "indexlib/config/index_config.h"
#include "indexlib/index/common/ErrorCode.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index_base/segment/segment_data.h"

DECLARE_REFERENCE_CLASS(file_system, FileReader);

namespace indexlib { namespace index {
class DictionaryReader;

class NormalIndexSegmentReader : public IndexSegmentReader
{
public:
    NormalIndexSegmentReader();
    ~NormalIndexSegmentReader();

public:
    virtual void Open(const config::IndexConfigPtr& indexConfig, const index_base::SegmentData& segmentData,
                      const NormalIndexSegmentReader* hintReader);
    // TODO: remove arg baseDocId
    bool GetSegmentPosting(const index::DictKeyInfo& key, docid64_t baseDocId, SegmentPosting& segPosting,
                           autil::mem_pool::Pool* sessionPool,
                           InvertedIndexSearchTracer* tracer = nullptr) const override;

    future_lite::coro::Lazy<index::Result<bool>> GetSegmentPostingAsync(const index::DictKeyInfo& key,
                                                                        docid_t baseDocId, SegmentPosting& segPosting,
                                                                        autil::mem_pool::Pool* sessionPool,
                                                                        file_system::ReadOption option) const noexcept;

    std::shared_ptr<DictionaryReader> GetDictionaryReader() const override { return mDictReader; }

    const index_base::SegmentData& GetSegmentData() { return mSegmentData; }

    size_t GetPostingFileLength() const
    {
        return mPostingReader.get() != nullptr ? mPostingReader->GetLogicLength() : 0;
    }

protected:
    void InnerGetSegmentPosting(dictvalue_t value, SegmentPosting& segPosting,
                                autil::mem_pool::Pool* sessionPool) const;

    future_lite::coro::Lazy<index::ErrorCode> GetSegmentPostingAsync(dictvalue_t value, SegmentPosting& segPosting,
                                                                     autil::mem_pool::Pool* sessionPool,
                                                                     file_system::ReadOption option) const noexcept;

    void Open(const config::IndexConfigPtr& indexConfig, const file_system::DirectoryPtr& directory,
              const NormalIndexSegmentReader* hintReader);

    DictionaryReader* CreateDictionaryReader(const config::IndexConfigPtr& indexConfig) const;

    file_system::FileReaderPtr GetSessionPostingFileReader(autil::mem_pool::Pool* sessionPool) const;

protected:
    std::shared_ptr<DictionaryReader> mDictReader;
    file_system::FileReaderPtr mPostingReader;
    uint8_t* mBaseAddr;
    LegacyIndexFormatOption mOption;
    index_base::SegmentData mSegmentData;

private:
    friend class NormalIndexSegmentReaderTest;
    friend class NormalIndexReaderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(NormalIndexSegmentReader);
}} // namespace indexlib::index

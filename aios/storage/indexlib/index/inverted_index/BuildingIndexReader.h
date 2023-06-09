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

#include "indexlib/document/normal/ModifiedTokens.h"
#include "indexlib/index/inverted_index/IndexSegmentReader.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/format/PostingFormatOption.h"

namespace indexlib::index {

class BuildingIndexReader
{
public:
    BuildingIndexReader(const PostingFormatOption& postingFormatOption) : _postingFormatOption(postingFormatOption) {}

    ~BuildingIndexReader() {}

public:
    void AddSegmentReader(docid_t baseDocId, const std::shared_ptr<IndexSegmentReader>& inMemSegReader);
    void GetSegmentPosting(const index::DictKeyInfo& key, std::vector<SegmentPosting>& segPostings,
                           autil::mem_pool::Pool* sessionPool, InvertedIndexSearchTracer* tracer = nullptr) const;
    size_t GetSegmentCount() const;
    std::vector<std::shared_ptr<DictionaryReader>> GetDictionaryReader();

    void Update(docid_t docId, const document::ModifiedTokens& tokens);
    void Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete);

private:
    std::pair<docid_t, IndexSegmentReader*> GetSegmentReader(docid_t docId);

protected:
    typedef std::pair<docid_t, std::shared_ptr<IndexSegmentReader>> SegmentReaderItem; //{baseDocId, segReader}
    typedef std::vector<SegmentReaderItem> SegmentReaders;

    SegmentReaders _innerSegReaders;
    PostingFormatOption _postingFormatOption;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlib::index

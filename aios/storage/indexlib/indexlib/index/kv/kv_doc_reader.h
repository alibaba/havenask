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
#ifndef __INDEXLIB_KV_DOC_READER_H
#define __INDEXLIB_KV_DOC_READER_H

#include "indexlib/index/kv/doc_reader_base.h"
#include "indexlib/index/kv/kv_segment_iterator.h"

namespace indexlib { namespace index {

class KVDocReader : public DocReaderBase
{
public:
    KVDocReader();
    virtual ~KVDocReader();

public:
    bool Init(const config::IndexPartitionSchemaPtr& schema, index_base::PartitionDataPtr partData,
              uint32_t targetShardId, int64_t currentTs, const std::string& ttlFieldName) override;
    bool Read(document::RawDocument* doc, uint32_t& timestamp) override;
    bool Seek(int64_t segmentIdx, int64_t offset) override;
    std::pair<int64_t, int64_t> GetCurrentOffset() const override;
    bool IsEof() const override;

private:
    void GenerateSegmentIds();
    bool CreateKVSegmentIteratorVec();

    util::Status ProcessOneDoc(KVSegmentIterator& segmentIterator, document::RawDocument* doc, uint32_t& timestamp);
    bool ReadSimpleValue(const autil::StringView& value, document::RawDocument* doc);
    bool ReadPackValue(const autil::StringView& value, document::RawDocument* doc);
    bool ReadValue(const keytype_t pkeyHash, const autil::StringView& value, document::RawDocument* doc);

private:
    std::vector<KVSegmentIteratorPtr> mSegmentIteratorVec;
    int64_t mLastValidSegmentIdx = 0;
    int64_t mLastValidIteratorOffset = 0;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(KVDocReader);
}} // namespace indexlib::index

#endif //__INDEXLIB_KV_DOC_READER_H

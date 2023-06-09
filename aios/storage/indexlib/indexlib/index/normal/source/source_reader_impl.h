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
#ifndef __INDEXLIB_SOURCE_READER_IMPL_H
#define __INDEXLIB_SOURCE_READER_IMPL_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/index/normal/source/in_mem_source_segment_reader.h"
#include "indexlib/index/normal/source/source_reader.h"
#include "indexlib/index/normal/source/source_segment_reader.h"
#include "indexlib/index_base/segment/segment_iterator.h"
#include "indexlib/indexlib.h"

namespace indexlib { namespace index {

class SourceReaderImpl : public SourceReader
{
public:
    SourceReaderImpl(const config::SourceSchemaPtr& sourceSchema);
    ~SourceReaderImpl();

public:
    bool Open(const index_base::PartitionDataPtr& partitionData, const SourceReader* hintReader) override;
    bool GetDocument(docid_t docId, document::SourceDocument* sourceDocument) const override;

private:
    bool DoGetDocument(docid_t docId, document::SourceDocument* sourceDocument) const;
    void InitBuildingSourceReader(const index_base::SegmentIteratorPtr& buildingIter);

private:
    docid_t mBuildingBaseDocId;
    std::vector<InMemSourceSegmentReaderPtr> mInMemSegReaders;
    std::vector<docid_t> mInMemSegmentBaseDocId;
    std::vector<SourceSegmentReaderPtr> mSegmentReaders;
    std::vector<uint64_t> mSegmentDocCount;
    std::vector<segmentid_t> mReaderSegmentIds;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(SourceReaderImpl);
}} // namespace indexlib::index

#endif //__INDEXLIB_SOURCE_READER_IMPL_H

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
#include "indexlib/index/normal/source/source_reader_impl.h"

#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"

using autil::StringView;
using std::string;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, SourceReaderImpl);

SourceReaderImpl::SourceReaderImpl(const config::SourceSchemaPtr& sourceSchema)
    : SourceReader(sourceSchema)
    , mBuildingBaseDocId(INVALID_DOCID)
{
}

SourceReaderImpl::~SourceReaderImpl() {}

bool SourceReaderImpl::Open(const index_base::PartitionDataPtr& partitionData, const SourceReader* hintReader)
{
    if (!mSourceSchema) {
        IE_LOG(ERROR, "source schema is empty");
        return false;
    }
    IE_LOG(DEBUG, "Begin opening source");
    index_base::PartitionSegmentIteratorPtr segIter = partitionData->CreateSegmentIterator();
    assert(segIter);
    index_base::SegmentIteratorPtr builtSegIter = segIter->CreateIterator(index_base::SegmentIteratorType::SIT_BUILT);
    assert(builtSegIter);
    std::unordered_map<segmentid_t, SourceSegmentReaderPtr> hintReaderMap;
    auto typedHintReader = dynamic_cast<const SourceReaderImpl*>(hintReader);
    if (typedHintReader) {
        for (size_t i = 0; i < typedHintReader->mReaderSegmentIds.size(); ++i) {
            hintReaderMap[typedHintReader->mReaderSegmentIds[i]] = typedHintReader->mSegmentReaders[i];
        }
    }

    while (builtSegIter->IsValid()) {
        const index_base::SegmentData& segData = builtSegIter->GetSegmentData();
        const std::shared_ptr<const index_base::SegmentInfo>& segmentInfo = segData.GetSegmentInfo();
        try {
            if (segmentInfo->docCount == 0) {
                builtSegIter->MoveToNext();
                continue;
            }
            segmentid_t segId = segData.GetSegmentId();
            SourceSegmentReaderPtr reader;
            auto iter = hintReaderMap.find(segId);
            if (iter != hintReaderMap.end()) {
                reader = iter->second;
            } else {
                reader.reset(new SourceSegmentReader(mSourceSchema));
                if (!reader->Open(segData, *segmentInfo)) {
                    IE_LOG(ERROR, "open source segmnet reader[%s] failed.",
                           segData.GetDirectory()->DebugString().c_str());
                    return false;
                }
            }
            mSegmentReaders.push_back(reader);
            mSegmentDocCount.push_back((uint64_t)segmentInfo->docCount);
            mReaderSegmentIds.push_back(segId);
            builtSegIter->MoveToNext();
        } catch (const util::ExceptionBase& e) {
            const file_system::DirectoryPtr& directory = segData.GetDirectory();
            IE_LOG(ERROR, "LoadSegment[%s] failed,  exception:%s", directory->DebugString().c_str(), e.what());
            throw;
        }
    }

    mBuildingBaseDocId = segIter->GetBuildingBaseDocId();
    index_base::SegmentIteratorPtr buildingSegIter =
        segIter->CreateIterator(index_base::SegmentIteratorType::SIT_BUILDING);
    InitBuildingSourceReader(buildingSegIter);
    IE_LOG(DEBUG, "End opening source");
    return true;
}

bool SourceReaderImpl::GetDocument(docid_t docId, document::SourceDocument* sourceDocument) const
{
    try {
        // TODO support get document by group ids
        return DoGetDocument(docId, sourceDocument);
    } catch (const util::ExceptionBase& e) {
        IE_LOG(ERROR, "GetDocument exception: %s", e.what());
    } catch (const std::exception& e) {
        IE_LOG(ERROR, "GetDocument exception: %s", e.what());
    } catch (...) {
        IE_LOG(ERROR, "GetDocument exception");
    }
    return false;
}

bool SourceReaderImpl::DoGetDocument(docid_t docId, document::SourceDocument* sourceDocument) const
{
    if (docId < 0) {
        return false;
    }
    // get from building segment
    if (docId >= mBuildingBaseDocId) {
        for (size_t i = 0; i < mInMemSegReaders.size(); i++) {
            docid_t curBaseDocId = mInMemSegmentBaseDocId[i];
            if (docId < curBaseDocId) {
                return false;
            }
            if (mInMemSegReaders[i]->GetDocument(docId - curBaseDocId, sourceDocument)) {
                return true;
            }
        }
        return false;
    }

    // get from onDisk segment
    docid_t baseDocId = 0;
    for (uint32_t i = 0; i < mSegmentDocCount.size(); i++) {
        if (docId < baseDocId + (docid_t)mSegmentDocCount[i]) {
            return mSegmentReaders[i]->GetDocument(docId - baseDocId, sourceDocument);
        }
        baseDocId += mSegmentDocCount[i];
    }
    return true;
}

void SourceReaderImpl::InitBuildingSourceReader(const index_base::SegmentIteratorPtr& buildingIter)
{
    if (!buildingIter) {
        return;
    }
    while (buildingIter->IsValid()) {
        index_base::InMemorySegmentPtr inMemorySegment = buildingIter->GetInMemSegment();
        index_base::InMemorySegmentReaderPtr segmentReader = inMemorySegment->GetSegmentReader();
        InMemSourceSegmentReaderPtr sourceReader = segmentReader->GetInMemSourceReader();
        assert(sourceReader);
        mInMemSegReaders.push_back(sourceReader);
        mInMemSegmentBaseDocId.push_back(buildingIter->GetBaseDocId());
        buildingIter->MoveToNext();
    }
}
}} // namespace indexlib::index

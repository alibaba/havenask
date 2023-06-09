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
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_index_reader.h"

#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/index/inverted_index/BuildingIndexReader.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingExpandData.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/InMemBitmapIndexSegmentReader.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"
#include "indexlib/index/inverted_index/format/dictionary/TieredDictionaryReader.h"
#include "indexlib/index/normal/framework/index_writer.h"
#include "indexlib/index/normal/framework/multi_field_index_segment_reader.h"
#include "indexlib/index/normal/inverted_index/format/index_format_option.h"
#include "indexlib/index/partition_info.h"
#include "indexlib/index_base/partition_data.h"
#include "indexlib/index_base/segment/in_memory_segment.h"
#include "indexlib/index_base/segment/in_memory_segment_reader.h"
#include "indexlib/index_base/segment/partition_segment_iterator.h"
#include "indexlib/index_base/segment/segment_data.h"
#include "indexlib/util/ExpandableBitmap.h"
#include "indexlib/util/Status2Exception.h"
#include "indexlib/util/memory_control/BuildResourceMetrics.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

using namespace indexlib::config;
using namespace indexlib::index_base;
using namespace indexlib::common;
using namespace indexlib::file_system;

using namespace indexlib::util;

namespace indexlib { namespace index { namespace legacy {
namespace {
using indexlib::index::BitmapPostingIterator;
}

IE_LOG_SETUP(index, BitmapIndexReader);

namespace {

struct ExpandDataTable {
    autil::mem_pool::UnsafePool pool;
    util::HashMap<dictkey_t, BitmapPostingExpandData*, autil::mem_pool::UnsafePool> table;
    BitmapPostingExpandData* nullTermData;

    ExpandDataTable() : table(&pool, HASHMAP_INIT_SIZE), nullTermData(nullptr) {}
    ExpandDataTable(const ExpandDataTable&) = delete;
    ExpandDataTable& operator=(const ExpandDataTable&) = delete;
    ~ExpandDataTable()
    {
        auto iter = table.CreateIterator();
        while (iter.HasNext()) {
            auto kvpair = iter.Next();
            kvpair.second->postingWriter->~BitmapPostingWriter();
        }
        table.Clear();
        if (nullTermData) {
            nullTermData->postingWriter->~BitmapPostingWriter();
        }
    }
};

BitmapPostingExpandData* CreateExpandData(ExpandDataTable* dataTable, DictionaryReader* dictReader,
                                          file_system::FileReader* postingReader,
                                          const index::DictKeyInfo& key) noexcept
{
    void* ptr = dataTable->pool.allocate(sizeof(BitmapPostingWriter));
    BitmapPostingWriter* writer = ::new (ptr) BitmapPostingWriter(&(dataTable->pool));
    ptr = dataTable->pool.allocate(sizeof(BitmapPostingExpandData));
    auto expandData = ::new (ptr) BitmapPostingExpandData();
    expandData->postingWriter = writer;
    dictvalue_t dictValue = 0;
    file_system::ReadOption option;
    if (dictReader and dictReader->Lookup(key, option, dictValue).ValueOrThrow()) {
        int64_t postingOffset = 0;
        ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);
        uint32_t postingLen;
        postingReader->Read(&postingLen, sizeof(uint32_t), postingOffset).GetOrThrow();
        postingOffset += sizeof(uint32_t);
        uint8_t* baseAddr = reinterpret_cast<uint8_t*>(postingReader->GetBaseAddress());

        util::ByteSlice singleSlice;
        singleSlice.data = baseAddr + postingOffset;
        singleSlice.size = postingLen;
        ByteSliceReader reader;
        reader.Open(&singleSlice);
        uint32_t pos = reader.Tell();
        TermMetaLoader tmLoader;
        tmLoader.Load(&reader, expandData->termMeta);
        uint32_t bitmapSize = reader.ReadUInt32();
        expandData->originalBitmapOffset = postingOffset + (reader.Tell() - pos);
        expandData->originalBitmapItemCount = bitmapSize * util::Bitmap::BYTE_SLOT_NUM;
    }
    return expandData;
}

bool TryUpdateInOriginalBitmap(uint8_t* segmentPostingBaseAddr, docid_t docId, bool isDelete,
                               BitmapPostingExpandData* expandData)
{
    if (!expandData or expandData->originalBitmapOffset < 0 or docId >= expandData->originalBitmapItemCount or
        !segmentPostingBaseAddr) {
        return false;
    }
    util::Bitmap bitmap;
    bitmap.MountWithoutRefreshSetCount(
        expandData->originalBitmapItemCount,
        reinterpret_cast<uint32_t*>(segmentPostingBaseAddr + expandData->originalBitmapOffset));
    df_t df = expandData->termMeta.GetDocFreq();
    if (isDelete) {
        if (bitmap.Reset(docId)) {
            expandData->termMeta.SetDocFreq(df - 1);
        }
    } else {
        if (bitmap.Set(docId)) {
            expandData->termMeta.SetDocFreq(df + 1);
        }
    }
    return true;
}
} // namespace

BitmapIndexReader::BitmapIndexReader() : mBuildResourceMetricsNode(nullptr), mTemperatureDocInfo(nullptr) {}

BitmapIndexReader::BitmapIndexReader(const BitmapIndexReader& other)
{
    mIndexConfig = other.mIndexConfig;
    mDictReaders = other.mDictReaders;
    mPostingReaders = other.mPostingReaders;
    mExpandResources = other.mExpandResources;
    mSegmentDocCount = other.mSegmentDocCount;
    mBaseDocIds = other.mBaseDocIds;
    mBuildingIndexReader = other.mBuildingIndexReader;
    mBuildResourceMetricsNode = other.mBuildResourceMetricsNode;
    mTemperatureDocInfo = other.mTemperatureDocInfo;
    mDictHasher = other.mDictHasher;
}

BitmapIndexReader::~BitmapIndexReader() {}

bool BitmapIndexReader::Open(const IndexConfigPtr& indexConfig, const PartitionDataPtr& partitionData)
{
    assert(indexConfig);
    mIndexConfig = indexConfig;
    mIndexFormatOption.Init(indexConfig);
    mTemperatureDocInfo = partitionData->GetTemperatureDocInfo();
    mDictHasher = IndexDictHasher(indexConfig->GetDictHashParams(), indexConfig->GetInvertedIndexType());
    return LoadSegments(partitionData);
}

void BitmapIndexReader::InitBuildResourceMetricsNode(util::BuildResourceMetrics* buildResourceMetrics)
{
    if (!buildResourceMetrics) {
        return;
    }
    mBuildResourceMetricsNode = buildResourceMetrics->AllocateNode();
    IE_LOG(INFO, "allocate build resource node [id:%d] for bitmap index reader [%s]",
           mBuildResourceMetricsNode->GetNodeId(), mIndexConfig->GetIndexName().c_str());
    UpdateBuildResourceMetrics();
}

void BitmapIndexReader::UpdateBuildResourceMetrics()
{
    if (mBuildResourceMetricsNode) {
        int64_t sum = 0;
        for (const auto& resource : mExpandResources) {
            if (resource and !resource->Empty()) {
                auto dataTable = resource->GetResource<ExpandDataTable>();
                assert(dataTable);
                sum += dataTable->pool.getUsedBytes();
            }
        }
        mBuildResourceMetricsNode->Update(BMT_CURRENT_MEMORY_USE, sum);
    }
}

index::ErrorCode BitmapIndexReader::FillSegmentPostingVector(const index::DictKeyInfo& key,
                                                             const DocIdRangeVector& ranges,
                                                             autil::mem_pool::Pool* sessionPool,
                                                             PostingFormatOption bitmapPostingFormatOption,
                                                             shared_ptr<SegmentPostingVector>& segPostings,
                                                             file_system::ReadOption option) const noexcept
{
    size_t reserveCount = mDictReaders.size();
    if (mBuildingIndexReader) {
        reserveCount += mBuildingIndexReader->GetSegmentCount();
    }
    segPostings->reserve(reserveCount);
    if (ranges.empty()) {
        for (uint32_t i = 0; i < mDictReaders.size(); i++) {
            SegmentPosting segPosting(bitmapPostingFormatOption);
            auto segmentResult = GetSegmentPosting(key, i, segPosting, option);
            if (!segmentResult.Ok()) {
                return segmentResult.GetErrorCode();
            }
            if (segmentResult.Value()) {
                segPostings->push_back(std::move(segPosting));
            }
        }
        if (mBuildingIndexReader) {
            mBuildingIndexReader->GetSegmentPosting(key, *segPostings, sessionPool, /*tracer*/ nullptr);
        }
    } else {
        size_t currentRangeIdx = 0;
        size_t currentSegmentIdx = 0;
        bool currentSegmentFilled = false;
        while (currentSegmentIdx < mDictReaders.size() && currentRangeIdx < ranges.size()) {
            const auto& range = ranges[currentRangeIdx];
            docid_t segBegin = mBaseDocIds[currentSegmentIdx];
            docid_t segEnd = mBaseDocIds[currentSegmentIdx] + mSegmentDocCount[currentSegmentIdx];
            if (segEnd <= range.first) {
                ++currentSegmentIdx;
                currentSegmentFilled = false;
                continue;
            }
            if (segBegin >= range.second) {
                ++currentRangeIdx;
                continue;
            }
            if (!currentSegmentFilled) {
                SegmentPosting segPosting(bitmapPostingFormatOption);
                auto segmentResult = GetSegmentPosting(key, currentSegmentIdx, segPosting, option);
                if (!segmentResult.Ok()) {
                    return segmentResult.GetErrorCode();
                }
                if (segmentResult.Value()) {
                    segPostings->push_back(std::move(segPosting));
                }
                currentSegmentFilled = true;
            }

            auto minEnd = std::min(segEnd, range.second);
            if (segEnd == minEnd) {
                ++currentSegmentIdx;
                currentSegmentFilled = false;
            }
            if (range.second == minEnd) {
                ++currentRangeIdx;
            }
        }
        if (currentRangeIdx < ranges.size() && mBuildingIndexReader) {
            mBuildingIndexReader->GetSegmentPosting(key, *segPostings, sessionPool);
        }
    }
    return index::ErrorCode::OK;
}

bool BitmapIndexReader::LoadSegments(const PartitionDataPtr& partitionData)
{
    mDictReaders.clear();
    mPostingReaders.clear();
    mSegmentDocCount.clear();
    mBaseDocIds.clear();

    PartitionSegmentIteratorPtr iter = partitionData->CreateSegmentIterator();
    assert(iter);
    while (iter->IsValid()) {
        if (iter->GetType() == SIT_BUILT) {
            const SegmentData& segmentData = iter->GetSegmentData();
            if (segmentData.GetSegmentInfo()->docCount == 0) {
                iter->MoveToNext();
                continue;
            }

            std::shared_ptr<DictionaryReader> dictReader;
            file_system::FileReaderPtr postingReader;
            file_system::ResourceFilePtr expandFile;
            try {
                LoadSegment(segmentData, &dictReader, &postingReader, &expandFile);
            } catch (const ExceptionBase& e) {
                IE_LOG(ERROR, "Load segment [%s] FAILED, reason: [%s]",
                       segmentData.GetDirectory()->DebugString().c_str(), e.what());
                throw;
            }
            mDictReaders.push_back(dictReader);
            mPostingReaders.push_back(postingReader);
            mExpandResources.push_back(expandFile);
            mSegmentDocCount.push_back((uint64_t)segmentData.GetSegmentInfo()->docCount);
            mBaseDocIds.push_back(iter->GetBaseDocId());
            mSegmentIds.push_back(iter->GetSegmentId());
        } else {
            assert(iter->GetType() == SIT_BUILDING);
            AddInMemSegment(iter->GetBaseDocId(), iter->GetInMemSegment());
            IE_LOG(DEBUG, "Add In-Memory SegmentReader for segment [%d], by index [%s]", iter->GetSegmentId(),
                   mIndexConfig->GetIndexName().c_str());
        }
        iter->MoveToNext();
    }
    return true;
}

void BitmapIndexReader::LoadSegment(const SegmentData& segmentData, std::shared_ptr<DictionaryReader>* dictReader,
                                    file_system::FileReaderPtr* postingReader, file_system::ResourceFilePtr* expandFile)
{
    dictReader->reset();
    postingReader->reset();

    file_system::DirectoryPtr directory = segmentData.GetIndexDirectory(mIndexConfig->GetIndexName(), true);
    if (directory->IsExist(BITMAP_DICTIONARY_FILE_NAME)) {
        dictReader->reset(new TieredDictionaryReader());
        auto status = (*dictReader)->Open(directory, BITMAP_DICTIONARY_FILE_NAME, false);
        THROW_IF_STATUS_ERROR(status);

        *postingReader =
            directory->CreateFileReader(BITMAP_POSTING_FILE_NAME, file_system::ReaderOption::Writable(FSOT_MEM_ACCESS));
    }
    if (mIndexConfig->IsIndexUpdatable()) {
        auto resource = directory->GetResourceFile(BITMAP_POSTING_EXPAND_FILE_NAME);
        if (!resource or resource->Empty()) {
            auto resourceTable = new ExpandDataTable();
            resource = directory->CreateResourceFile(BITMAP_POSTING_EXPAND_FILE_NAME);
            assert(resource);
            resource->Reset(resourceTable);
        }
        *expandFile = resource;
    }
}

BitmapIndexReader* BitmapIndexReader::Clone() const { return new BitmapIndexReader(*this); }

index::Result<PostingIterator*> BitmapIndexReader::Lookup(const index::DictKeyInfo& key, const DocIdRangeVector& ranges,
                                                          uint32_t statePoolSize, autil::mem_pool::Pool* sessionPool,
                                                          file_system::ReadOption option) const noexcept
{
    LegacyIndexFormatOption indexFormatOption;
    indexFormatOption.Init(mIndexConfig);
    PostingFormatOption bitmapPostingFormatOption =
        indexFormatOption.GetPostingFormatOption().GetBitmapPostingFormatOption();

    shared_ptr<SegmentPostingVector> segPostings(new SegmentPostingVector);
    auto ec = FillSegmentPostingVector(key, ranges, sessionPool, bitmapPostingFormatOption, segPostings, option);
    if (ec != index::ErrorCode::OK) {
        return ec;
    }

    BitmapPostingIterator* bitmapIt = CreateBitmapPostingIterator(sessionPool);
    assert(bitmapIt);
    if (bitmapIt->Init(segPostings, statePoolSize)) {
        return bitmapIt;
    }
    IE_POOL_COMPATIBLE_DELETE_CLASS(sessionPool, bitmapIt);
    return nullptr;
}

void BitmapIndexReader::Update(docid_t docId, const index::DictKeyInfo& key, bool isDelete)
{
    if (mExpandResources.empty()) {
        return;
    }
    assert(mExpandResources.size() == mSegmentDocCount.size());
    docid_t baseDocId = 0;
    int64_t segmentIdx = -1;
    for (size_t i = 0; i < mSegmentDocCount.size(); i++) {
        uint64_t docCount = mSegmentDocCount[i];
        if (docId < baseDocId + (docid_t)docCount) {
            segmentIdx = i;
            break;
        }
        baseDocId += docCount;
    }
    if (segmentIdx == -1) {
        if (mBuildingIndexReader) {
            mBuildingIndexReader->Update(docId, key, isDelete);
        } else {
            IE_LOG(ERROR, "BuildingIndexReader not initialized.")
        }

        return;
    }
    assert(mExpandResources[segmentIdx] and !mExpandResources[segmentIdx]->Empty());
    auto dataTable = mExpandResources[segmentIdx]->GetResource<ExpandDataTable>();
    assert(dataTable);

    BitmapPostingExpandData* expandData = nullptr;
    bool needInsertNewData = false;
    uint64_t buildingHashKey = InvertedIndexUtil::GetRetrievalHashKey(mIndexFormatOption.IsNumberIndex(), key.GetKey());
    if (key.IsNull()) {
        if (!mIndexConfig->SupportNull()) {
            return;
        }
        if (!dataTable->nullTermData) {
            dataTable->nullTermData =
                CreateExpandData(dataTable, mDictReaders[segmentIdx].get(), mPostingReaders[segmentIdx].get(), key);
        }
        expandData = dataTable->nullTermData;
    } else {
        BitmapPostingExpandData** pData = dataTable->table.Find(buildingHashKey);
        if (!pData) {
            expandData =
                CreateExpandData(dataTable, mDictReaders[segmentIdx].get(), mPostingReaders[segmentIdx].get(), key);
            needInsertNewData = true;
        } else {
            expandData = *pData;
        }
    }

    uint8_t* segmentPostingFileBaseAddr = nullptr;
    if (mPostingReaders[segmentIdx]) {
        segmentPostingFileBaseAddr = reinterpret_cast<uint8_t*>(mPostingReaders[segmentIdx]->GetBaseAddress());
    }
    if (!TryUpdateInOriginalBitmap(segmentPostingFileBaseAddr, docId - baseDocId, isDelete, expandData)) {
        int64_t curDf = expandData->postingWriter->GetDF();
        expandData->postingWriter->Update(docId - baseDocId - expandData->originalBitmapItemCount, isDelete);
        int64_t dfDiff = static_cast<int64_t>(expandData->postingWriter->GetDF()) - curDf;
        int64_t df = expandData->termMeta.GetDocFreq();
        expandData->termMeta.SetDocFreq(df + dfDiff);
        UpdateBuildResourceMetrics();
    }
    if (needInsertNewData) {
        dataTable->table.Insert(buildingHashKey, expandData);
    }
}

index::Result<bool> BitmapIndexReader::GetSegmentPosting(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                         SegmentPosting& segPosting,
                                                         file_system::ReadOption option) const noexcept
{
    auto getResult = GetSegmentPostingFromIndex(key, segmentIdx, segPosting, option);
    if (!getResult.Ok()) {
        return getResult.GetErrorCode();
    }
    bool found = getResult.Value();
    if (!mExpandResources.empty() and mExpandResources[segmentIdx] and !mExpandResources[segmentIdx]->Empty()) {
        auto dataTable = mExpandResources[segmentIdx]->GetResource<ExpandDataTable>();
        assert(dataTable);
        BitmapPostingExpandData** pData = nullptr;
        if (key.IsNull()) {
            pData = &(dataTable->nullTermData);
        } else {
            auto retrievalHashKey =
                InvertedIndexUtil::GetRetrievalHashKey(mIndexFormatOption.IsNumberIndex(), key.GetKey());
            pData = dataTable->table.Find(retrievalHashKey);
        }
        if (pData) {
            if (!found) {
                segPosting.Init(mBaseDocIds[segmentIdx], mSegmentDocCount[segmentIdx]);
            }
            segPosting.SetResource(*pData);
            found = true;
        }
    }
    return found;
}

index::Result<bool> BitmapIndexReader::GetSegmentPostingFromIndex(const index::DictKeyInfo& key, uint32_t segmentIdx,
                                                                  SegmentPosting& segPosting,
                                                                  file_system::ReadOption option) const noexcept
{
    if (!mDictReaders[segmentIdx]) {
        return false;
    }
    dictvalue_t dictValue = 0;
    auto dictResult = mDictReaders[segmentIdx]->Lookup(key, option, dictValue);
    if (!dictResult.Ok()) {
        return dictResult.GetErrorCode();
    }
    if (!dictResult.Value()) {
        return false;
    }

    int64_t postingOffset = 0;
    ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);

    uint32_t postingLen;
    mPostingReaders[segmentIdx]->Read(&postingLen, sizeof(uint32_t), postingOffset).GetOrThrow();

    postingOffset += sizeof(uint32_t);

    uint8_t* baseAddr = (uint8_t*)mPostingReaders[segmentIdx]->GetBaseAddress();
    assert(baseAddr);
    segPosting.Init(baseAddr + postingOffset, postingLen, mBaseDocIds[segmentIdx], mSegmentDocCount[segmentIdx],
                    dictValue);
    return true;
}

void BitmapIndexReader::AddInMemSegment(docid_t baseDocId, const InMemorySegmentPtr& inMemSegment)
{
    if (!inMemSegment) {
        return;
    }

    InMemorySegmentReaderPtr reader = inMemSegment->GetSegmentReader();
    const string& indexName = mIndexConfig->GetIndexName();
    const auto& segmentReader = reader->GetMultiFieldIndexSegmentReader()->GetIndexSegmentReader(indexName);
    AddInMemSegmentReader(baseDocId, segmentReader->GetBitmapSegmentReader());
}

void BitmapIndexReader::AddInMemSegmentReader(docid_t baseDocId,
                                              const InMemBitmapIndexSegmentReaderPtr& bitmapSegReader)
{
    if (!mBuildingIndexReader) {
        LegacyIndexFormatOption indexFormatOption;
        indexFormatOption.Init(mIndexConfig);
        PostingFormatOption bitmapPostingFormatOption =
            indexFormatOption.GetPostingFormatOption().GetBitmapPostingFormatOption();
        mBuildingIndexReader.reset(new BuildingIndexReader(bitmapPostingFormatOption));
    }
    mBuildingIndexReader->AddSegmentReader(baseDocId, bitmapSegReader);
}

BitmapPostingIterator* BitmapIndexReader::CreateBitmapPostingIterator(Pool* sessionPool) const
{
    return IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool, BitmapPostingIterator, mIndexConfig->GetOptionFlag(), sessionPool);
}

BitmapIndexReader::TermUpdater BitmapIndexReader::GetTermUpdater(segmentid_t targetSegmentId,
                                                                 index::DictKeyInfo termKey)
{
    if (mExpandResources.empty()) {
        return TermUpdater(/*segmentPostingFileBaseAddr*/ nullptr,
                           /*expandData*/ nullptr,
                           /*bitmapReader*/ nullptr);
    }
    int64_t segmentIdx = -1;
    for (size_t i = 0; i < mSegmentIds.size(); i++) {
        if (targetSegmentId == mSegmentIds[i]) {
            segmentIdx = i;
            break;
        }
    }
    auto dataTable = mExpandResources[segmentIdx]->GetResource<ExpandDataTable>();
    BitmapPostingExpandData* expandData = nullptr;
    assert(dataTable);
    if (termKey.IsNull()) {
        if (!dataTable->nullTermData) {
            dataTable->nullTermData =
                CreateExpandData(dataTable, mDictReaders[segmentIdx].get(), mPostingReaders[segmentIdx].get(), termKey);
        }
        expandData = dataTable->nullTermData;
    } else {
        auto buildingHashKey =
            InvertedIndexUtil::GetRetrievalHashKey(mIndexFormatOption.IsNumberIndex(), termKey.GetKey());
        BitmapPostingExpandData** pData = dataTable->table.Find(buildingHashKey);
        if (!pData) {
            expandData =
                CreateExpandData(dataTable, mDictReaders[segmentIdx].get(), mPostingReaders[segmentIdx].get(), termKey);
            dataTable->table.Insert(buildingHashKey, expandData);
        } else {
            expandData = *pData;
        }
    }
    assert(expandData);
    uint8_t* postingFileBase = nullptr;
    if (mPostingReaders[segmentIdx]) {
        postingFileBase = reinterpret_cast<uint8_t*>(mPostingReaders[segmentIdx]->GetBaseAddress());
    }
    return TermUpdater(postingFileBase, expandData, this);
}

BitmapIndexReader::TermUpdater::TermUpdater(uint8_t* segmentPostingFileBaseAddr, BitmapPostingExpandData* expandData,
                                            BitmapIndexReader* reader)
    : _segmentPostingFileBaseAddr(segmentPostingFileBaseAddr)
    , _expandData(expandData)
    , _reader(reader)
{
}

BitmapIndexReader::TermUpdater::TermUpdater(BitmapIndexReader::TermUpdater&& other)
    : _segmentPostingFileBaseAddr(std::exchange(other._segmentPostingFileBaseAddr, nullptr))
    , _expandData(std::exchange(other._expandData, nullptr))
    , _reader(std::exchange(other._reader, nullptr))
{
}

BitmapIndexReader::TermUpdater& BitmapIndexReader::TermUpdater::operator=(BitmapIndexReader::TermUpdater&& other)
{
    if (&other != this) {
        _segmentPostingFileBaseAddr = std::exchange(other._segmentPostingFileBaseAddr, nullptr);
        _expandData = std::exchange(other._expandData, nullptr);
        _reader = std::exchange(other._reader, nullptr);
    }
    return *this;
}

BitmapIndexReader::TermUpdater::~TermUpdater()
{
    if (_reader) {
        _reader->UpdateBuildResourceMetrics();
    }
}

void BitmapIndexReader::TermUpdater::Update(docid_t localDocId, bool isDelete)
{
    // patch only
    if (!TryUpdateInOriginalBitmap(_segmentPostingFileBaseAddr, localDocId, isDelete, _expandData)) {
        int64_t curDf = _expandData->postingWriter->GetDF();
        _expandData->postingWriter->Update(localDocId - _expandData->originalBitmapItemCount, isDelete);
        int64_t dfDiff = static_cast<int64_t>(_expandData->postingWriter->GetDF()) - curDf;
        int64_t df = _expandData->termMeta.GetDocFreq();
        _expandData->termMeta.SetDocFreq(df + dfDiff);
    }
}

}}} // namespace indexlib::index::legacy

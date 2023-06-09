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
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapLeafReader.h"

#include "indexlib/file_system/file/ResourceFile.h"
#include "indexlib/index/inverted_index/InvertedIndexSearchTracer.h"
#include "indexlib/index/inverted_index/InvertedIndexUtil.h"
#include "indexlib/index/inverted_index/SegmentPosting.h"
#include "indexlib/index/inverted_index/format/ShortListOptimizeUtil.h"
#include "indexlib/index/inverted_index/format/TermMetaLoader.h"
#include "indexlib/index/inverted_index/format/dictionary/DictionaryReader.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, BitmapLeafReader);

BitmapLeafReader::BitmapLeafReader(const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConfig,
                                   const std::shared_ptr<DictionaryReader>& dictReader,
                                   const std::shared_ptr<file_system::FileReader>& postingReader,
                                   const std::shared_ptr<file_system::ResourceFile>& expandResource, uint64_t docCount)
    : _indexConfig(indexConfig)
    , _dictReader(dictReader)
    , _postingReader(postingReader)
    , _expandResource(expandResource)
    , _docCount(docCount)
{
    _indexFormatOption.Init(_indexConfig);
}

Result<bool> BitmapLeafReader::GetSegmentPosting(const DictKeyInfo& key, docid_t baseDocId, SegmentPosting& segPosting,
                                                 file_system::ReadOption option,
                                                 InvertedIndexSearchTracer* tracer) const noexcept
{
    auto getResult = GetSegmentPostingFromIndex(key, baseDocId, segPosting, option, tracer);
    if (!getResult.Ok()) {
        return getResult.GetErrorCode();
    }
    bool found = getResult.Value();
    if (_expandResource and !_expandResource->Empty()) {
        auto dataTable = _expandResource->GetResource<ExpandDataTable>();
        assert(dataTable);
        BitmapPostingExpandData** pData = nullptr;
        if (key.IsNull()) {
            pData = &(dataTable->nullTermData);
        } else {
            auto retrievalHashKey =
                InvertedIndexUtil::GetRetrievalHashKey(_indexFormatOption.IsNumberIndex(), key.GetKey());
            pData = dataTable->table.Find(retrievalHashKey);
        }
        if (pData) {
            if (!found) {
                segPosting.Init(baseDocId, _docCount);
            }
            segPosting.SetResource(*pData);
            found = true;
        }
    }
    return found;
}

Result<bool> BitmapLeafReader::GetSegmentPostingFromIndex(const DictKeyInfo& key, docid_t baseDocId,
                                                          SegmentPosting& segPosting, file_system::ReadOption option,
                                                          InvertedIndexSearchTracer* tracer) const noexcept
{
    if (!_dictReader) {
        return false;
    }
    if (tracer) {
        tracer->IncDictionaryLookupCount();
        option.blockCounter = tracer->GetDictionaryBlockCacheCounter();
    }
    dictvalue_t dictValue = 0;
    auto dictResult = _dictReader->Lookup(key, option, dictValue);
    if (!dictResult.Ok()) {
        return dictResult.GetErrorCode();
    }
    if (!dictResult.Value()) {
        return false;
    }
    if (tracer) {
        tracer->IncDictionaryHitCount();
    }

    int64_t postingOffset = 0;
    ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);

    uint32_t postingLen;
    _postingReader->Read(&postingLen, sizeof(uint32_t), postingOffset).GetOrThrow();

    postingOffset += sizeof(uint32_t);

    uint8_t* baseAddr = (uint8_t*)_postingReader->GetBaseAddress();
    assert(baseAddr);
    segPosting.Init(baseAddr + postingOffset, postingLen, baseDocId, _docCount, dictValue);

    return true;
}

void BitmapLeafReader::Update(docid_t docId, const DictKeyInfo& key, bool isDelete)
{
    assert(_expandResource && (!_expandResource->Empty()));
    auto dataTable = _expandResource->GetResource<ExpandDataTable>();
    assert(dataTable);

    BitmapPostingExpandData* expandData = nullptr;
    bool needInsertNewData = false;
    uint64_t buildingHashKey = InvertedIndexUtil::GetRetrievalHashKey(_indexFormatOption.IsNumberIndex(), key.GetKey());
    if (key.IsNull()) {
        if (!_indexConfig->SupportNull()) {
            return;
        }
        if (!dataTable->nullTermData) {
            dataTable->nullTermData = CreateExpandData(dataTable, _dictReader.get(), _postingReader.get(), key);
        }
        expandData = dataTable->nullTermData;
    } else {
        BitmapPostingExpandData** pData = dataTable->table.Find(buildingHashKey);
        if (!pData) {
            expandData = CreateExpandData(dataTable, _dictReader.get(), _postingReader.get(), key);
            needInsertNewData = true;
        } else {
            expandData = *pData;
        }
    }

    uint8_t* segmentPostingFileBaseAddr = nullptr;
    if (_postingReader) {
        segmentPostingFileBaseAddr = reinterpret_cast<uint8_t*>(_postingReader->GetBaseAddress());
    }
    if (!TryUpdateInOriginalBitmap(segmentPostingFileBaseAddr, docId, isDelete, expandData)) {
        int64_t curDf = expandData->postingWriter->GetDF();
        expandData->postingWriter->Update(docId - expandData->originalBitmapItemCount, isDelete);
        int64_t dfDiff = static_cast<int64_t>(expandData->postingWriter->GetDF()) - curDf;
        int64_t df = expandData->termMeta.GetDocFreq();
        expandData->termMeta.SetDocFreq(df + dfDiff);
    }
    if (needInsertNewData) {
        dataTable->table.Insert(buildingHashKey, expandData);
    }
}

BitmapPostingExpandData* BitmapLeafReader::CreateExpandData(ExpandDataTable* dataTable, DictionaryReader* dictReader,
                                                            file_system::FileReader* postingReader,
                                                            const DictKeyInfo& key) noexcept
{
    void* ptr = dataTable->pool.allocate(sizeof(BitmapPostingWriter));
    BitmapPostingWriter* writer = ::new (ptr) BitmapPostingWriter(&(dataTable->pool));
    ptr = dataTable->pool.allocate(sizeof(BitmapPostingExpandData));
    auto expandData = ::new (ptr) BitmapPostingExpandData();
    expandData->postingWriter = writer;
    dictvalue_t dictValue = 0;
    file_system::ReadOption option;
    if (dictReader && dictReader->Lookup(key, option, dictValue).ValueOrThrow()) {
        int64_t postingOffset = 0;
        ShortListOptimizeUtil::GetOffset(dictValue, postingOffset);
        uint32_t postingLen;
        postingReader->Read(&postingLen, sizeof(uint32_t), postingOffset).GetOrThrow();
        postingOffset += sizeof(uint32_t);
        uint8_t* baseAddr = reinterpret_cast<uint8_t*>(postingReader->GetBaseAddress());

        util::ByteSlice singleSlice;
        singleSlice.data = baseAddr + postingOffset;
        singleSlice.size = postingLen;
        file_system::ByteSliceReader reader;
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

bool BitmapLeafReader::TryUpdateInOriginalBitmap(uint8_t* segmentPostingBaseAddr, docid_t docId, bool isDelete,
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

size_t BitmapLeafReader::EvaluateCurrentMemUsed() const
{
    size_t totalMemUse = sizeof(*this);
    if (_postingReader && _postingReader->IsMemLock()) {
        totalMemUse += _postingReader->GetLength();
    }
    if (_dictReader) {
        totalMemUse += _dictReader->EstimateMemUsed();
    }
    if (_expandResource && (!_expandResource->Empty())) {
        auto dataTable = _expandResource->GetResource<ExpandDataTable>();
        assert(dataTable);
        totalMemUse += dataTable->pool.getUsedBytes();
    }
    return totalMemUse;
}

} // namespace indexlib::index

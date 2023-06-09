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
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/AdaptiveBitmapIndexWriter.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/adaptive_bitmap/AdaptiveBitmapTrigger.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/Status2Exception.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, AdaptiveBitmapIndexWriter);

AdaptiveBitmapIndexWriter::AdaptiveBitmapIndexWriter(
    const std::shared_ptr<AdaptiveBitmapTrigger>& trigger,
    const std::shared_ptr<indexlibv2::config::InvertedIndexConfig>& indexConf,
    const std::shared_ptr<file_system::RelocatableFolder>& adaptiveDictFolder)
    : _adaptiveBitmapTrigger(trigger)
    , _indexConfig(indexConf)
    , _adaptiveDictFolder(adaptiveDictFolder)
{
    assert(_indexConfig);
}

void AdaptiveBitmapIndexWriter::Init(const std::shared_ptr<DictionaryWriter>& dictWriter,
                                     const std::shared_ptr<file_system::FileWriter>& postingWriter)
{
    _dictionaryWriter = std::dynamic_pointer_cast<BitmapDictionaryWriter>(dictWriter);
    _postingWriter = postingWriter;
    assert(_dictionaryWriter);
    assert(_postingWriter);
}

bool AdaptiveBitmapIndexWriter::NeedAdaptiveBitmap(const index::DictKeyInfo& dictKey,
                                                   const std::shared_ptr<PostingMerger>& postingMerger)
{
    return _adaptiveBitmapTrigger->NeedGenerateAdaptiveBitmap(dictKey, postingMerger);
}

void AdaptiveBitmapIndexWriter::AddPosting(const index::DictKeyInfo& dictKey, termpayload_t termPayload,
                                           const std::shared_ptr<PostingIterator>& postingIt)
{
    std::vector<docid_t> docIds;
    CollectDocIds(postingIt, docIds);
    if (0 == docIds.size()) {
        AUTIL_LOG(ERROR, "dictKey [%s] has no doc list", dictKey.ToString().c_str());
        return;
    }

    auto writer = std::make_shared<BitmapPostingWriter>();
    for (size_t i = 0; i < docIds.size(); i++) {
        writer->AddPosition(0, 0);
        writer->EndDocument(docIds[i], 0);
    }

    writer->SetTermPayload(termPayload);
    DumpPosting(dictKey, writer);
    _adaptiveDictKeys.push_back(dictKey);
}

void AdaptiveBitmapIndexWriter::EndPosting() { DumpAdaptiveDictFile(); }

int64_t AdaptiveBitmapIndexWriter::EstimateMemoryUse(uint32_t docCount) const
{
    int64_t size = sizeof(*this);
    size += docCount * sizeof(docid_t) * 2;
    size += file_system::WriterOption::DEFAULT_BUFFER_SIZE * 2; // data and dict
    return size;
}

void AdaptiveBitmapIndexWriter::CollectDocIds(const std::shared_ptr<PostingIterator>& postingIt,
                                              std::vector<docid_t>& docIds)
{
    docid_t docId = 0;
    while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID) {
        docIds.push_back(docId);
    }
}

void AdaptiveBitmapIndexWriter::DumpPosting(const index::DictKeyInfo& key,
                                            const std::shared_ptr<BitmapPostingWriter>& writer)
{
    uint64_t offset = _postingWriter->GetLength();
    _dictionaryWriter->AddItem(key, offset);

    uint32_t postingLen = writer->GetDumpLength();

    // TODO(yonghao.fyh) : remove get or throw
    _postingWriter->Write((void*)(&postingLen), sizeof(uint32_t)).GetOrThrow();
    writer->Dump(_postingWriter);
}

void AdaptiveBitmapIndexWriter::DumpAdaptiveDictFile()
{
    std::shared_ptr<config::HighFrequencyVocabulary> vocabulary =
        config::HighFreqVocabularyCreator::CreateAdaptiveVocabulary(
            _indexConfig->GetIndexName(), _indexConfig->GetInvertedIndexType(),
            _indexConfig->GetNullTermLiteralString(), _indexConfig->GetDictConfig(), _indexConfig->GetDictHashParams());
    assert(vocabulary);
    for (size_t i = 0; i < _adaptiveDictKeys.size(); i++) {
        vocabulary->AddKey(_adaptiveDictKeys[i]);
    }
    auto status = vocabulary->DumpTerms(_adaptiveDictFolder);
    THROW_IF_STATUS_ERROR(status);
}

} // namespace indexlib::index

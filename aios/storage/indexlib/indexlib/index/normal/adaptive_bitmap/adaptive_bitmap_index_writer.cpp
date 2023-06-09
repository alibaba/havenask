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
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"

#include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/index/inverted_index/PostingIterator.h"
#include "indexlib/index/inverted_index/builtin_index/bitmap/BitmapPostingWriter.h"
#include "indexlib/index/inverted_index/config/DictionaryConfig.h"
#include "indexlib/index/inverted_index/config/HighFreqVocabularyCreator.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger.h"
#include "indexlib/util/PathUtil.h"
#include "indexlib/util/Status2Exception.h"

using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::config;
using namespace indexlib::file_system;
using namespace std;

namespace indexlib { namespace index { namespace legacy {
IE_LOG_SETUP(index, AdaptiveBitmapIndexWriter);

AdaptiveBitmapIndexWriter::AdaptiveBitmapIndexWriter(const AdaptiveBitmapTriggerPtr& trigger,
                                                     const IndexConfigPtr& indexConf,
                                                     const ArchiveFolderPtr& adaptiveDictFolder)
    : mAdaptiveBitmapTrigger(trigger)
    , mIndexConfig(indexConf)
    , mAdaptiveDictFolder(adaptiveDictFolder)
{
    assert(mIndexConfig);
}

AdaptiveBitmapIndexWriter::~AdaptiveBitmapIndexWriter() {}

void AdaptiveBitmapIndexWriter::Init(const std::shared_ptr<DictionaryWriter>& dictWriter,
                                     const file_system::FileWriterPtr& postingWriter)
{
    mDictionaryWriter = DYNAMIC_POINTER_CAST(BitmapDictionaryWriter, dictWriter);
    mPostingWriter = postingWriter;
    assert(mDictionaryWriter);
    assert(mPostingWriter);
}

bool AdaptiveBitmapIndexWriter::NeedAdaptiveBitmap(const index::DictKeyInfo& dictKey,
                                                   const PostingMergerPtr& postingMerger)
{
    return mAdaptiveBitmapTrigger->NeedGenerateAdaptiveBitmap(dictKey, postingMerger);
}

void AdaptiveBitmapIndexWriter::AddPosting(const index::DictKeyInfo& dictKey, termpayload_t termPayload,
                                           const std::shared_ptr<PostingIterator>& postingIt)
{
    vector<docid_t> docIds;
    CollectDocIds(postingIt, docIds);
    if (0 == docIds.size()) {
        IE_LOG(ERROR, "dictKey [%s] has no doc list", dictKey.ToString().c_str());
        return;
    }

    shared_ptr<BitmapPostingWriter> writer(new BitmapPostingWriter());
    for (size_t i = 0; i < docIds.size(); i++) {
        writer->AddPosition(0, 0);
        writer->EndDocument(docIds[i], 0);
    }

    writer->SetTermPayload(termPayload);
    DumpPosting(dictKey, writer);
    mAdaptiveDictKeys.push_back(dictKey);
}

void AdaptiveBitmapIndexWriter::EndPosting() { DumpAdaptiveDictFile(); }

void AdaptiveBitmapIndexWriter::DumpPosting(const index::DictKeyInfo& key,
                                            const shared_ptr<BitmapPostingWriter>& writer)
{
    uint64_t offset = mPostingWriter->GetLength();
    mDictionaryWriter->AddItem(key, offset);

    uint32_t postingLen = writer->GetDumpLength();
    mPostingWriter->Write((void*)(&postingLen), sizeof(uint32_t)).GetOrThrow();
    writer->Dump(mPostingWriter);
}

void AdaptiveBitmapIndexWriter::CollectDocIds(const std::shared_ptr<PostingIterator>& postingIt,
                                              vector<docid_t>& docIds)
{
    docid_t docId = 0;
    while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID) {
        docIds.push_back(docId);
    }
}

void AdaptiveBitmapIndexWriter::DumpAdaptiveDictFile()
{
    std::shared_ptr<HighFrequencyVocabulary> vocabulary = HighFreqVocabularyCreator::CreateAdaptiveVocabulary(
        mIndexConfig->GetIndexName(), mIndexConfig->GetInvertedIndexType(), mIndexConfig->GetNullTermLiteralString(),
        mIndexConfig->GetDictConfig(), mIndexConfig->GetDictHashParams());
    assert(vocabulary);
    for (size_t i = 0; i < mAdaptiveDictKeys.size(); i++) {
        vocabulary->AddKey(mAdaptiveDictKeys[i]);
    }
    auto status = vocabulary->DumpTerms(mAdaptiveDictFolder);
    THROW_IF_STATUS_ERROR(status);
}

int64_t AdaptiveBitmapIndexWriter::EstimateMemoryUse(uint32_t docCount) const
{
    int64_t size = sizeof(*this);
    size += docCount * sizeof(docid_t) * 2;
    size += file_system::WriterOption::DEFAULT_BUFFER_SIZE * 2; // data and dict
    return size;
}
}}} // namespace indexlib::index::legacy

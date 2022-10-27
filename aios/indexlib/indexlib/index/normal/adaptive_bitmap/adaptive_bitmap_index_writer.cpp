#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_index_writer.h"
#include "indexlib/index/normal/adaptive_bitmap/adaptive_bitmap_trigger.h"
#include "indexlib/index/normal/inverted_index/truncate/truncate_posting_iterator_creator.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/config/dictionary_config.h"
#include "indexlib/config/high_freq_vocabulary_creator.h"
#include "indexlib/util/path_util.h"

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(config);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(storage);
using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, AdaptiveBitmapIndexWriter);

AdaptiveBitmapIndexWriter::AdaptiveBitmapIndexWriter(
        const AdaptiveBitmapTriggerPtr& trigger,
        const IndexConfigPtr& indexConf,
        const ArchiveFolderPtr& adaptiveDictFolder)
    : mAdaptiveBitmapTrigger(trigger)
    , mIndexConfig(indexConf)
    , mAdaptiveDictFolder(adaptiveDictFolder)
{
    assert(mIndexConfig);
}

AdaptiveBitmapIndexWriter::~AdaptiveBitmapIndexWriter() 
{
}

void AdaptiveBitmapIndexWriter::Init(
        const DictionaryWriterPtr& dictWriter,
        const file_system::FileWriterPtr& postingWriter)
{
    mDictionaryWriter = DYNAMIC_POINTER_CAST(
            BitmapDictionaryWriter, dictWriter);
    mPostingWriter = postingWriter;
    assert(mDictionaryWriter);
    assert(mPostingWriter);
}

bool AdaptiveBitmapIndexWriter::NeedAdaptiveBitmap(
        dictkey_t dictKey,
        const PostingMergerPtr& postingMerger)
{
    return mAdaptiveBitmapTrigger->NeedGenerateAdaptiveBitmap(
            dictKey, postingMerger);
}

void AdaptiveBitmapIndexWriter::AddPosting(
        dictkey_t dictKey, termpayload_t termPayload,
        const PostingIteratorPtr& postingIt)
{
    vector<docid_t> docIds;
    CollectDocIds(postingIt, docIds);
    if (0 == docIds.size())
    {
        IE_LOG(ERROR, "dictKey [%d] has no doc list", (int)dictKey);
        return;
    }

    BitmapPostingWriterPtr writer(new BitmapPostingWriter());
    for (size_t i = 0; i < docIds.size(); i++)
    {
        writer->AddPosition(0, 0);
        writer->EndDocument(docIds[i], 0);
    }

    writer->SetTermPayload(termPayload);
    DumpPosting(dictKey, writer);
    mAdaptiveDictKeys.push_back(dictKey);
}

void AdaptiveBitmapIndexWriter::EndPosting()
{
    DumpAdaptiveDictFile();
}
    
void AdaptiveBitmapIndexWriter::DumpPosting(
        uint64_t key, 
        const BitmapPostingWriterPtr& writer)
{
    uint64_t offset = mPostingWriter->GetLength();
    mDictionaryWriter->AddItem(key, offset);

    uint32_t postingLen = writer->GetDumpLength();
    mPostingWriter->Write((void*)(&postingLen), sizeof(uint32_t));
    writer->Dump(mPostingWriter);
}

void AdaptiveBitmapIndexWriter::CollectDocIds(
        const PostingIteratorPtr& postingIt,
        vector<docid_t>& docIds)
{
    docid_t docId = 0;
    while ((docId = postingIt->SeekDoc(docId)) != INVALID_DOCID)
    {
        docIds.push_back(docId);
    }
}

void AdaptiveBitmapIndexWriter::DumpAdaptiveDictFile()
{
    HighFrequencyVocabularyPtr vocabulary = 
        HighFreqVocabularyCreator::CreateAdaptiveVocabulary(mIndexConfig->GetIndexName(),
                mIndexConfig->GetIndexType(), mIndexConfig->GetDictConfig());
    assert(vocabulary);
    for (size_t i = 0; i < mAdaptiveDictKeys.size(); i++)
    {
        vocabulary->AddKey(mAdaptiveDictKeys[i]);
    }
    vocabulary->DumpTerms(mAdaptiveDictFolder);
}

int64_t AdaptiveBitmapIndexWriter::EstimateMemoryUse(uint32_t docCount) const
{
    int64_t size = sizeof(*this);
    size += docCount * sizeof(docid_t) * 2;
    size += file_system::FSWriterParam::DEFAULT_BUFFER_SIZE * 2; // data and dict
    return size;
}

IE_NAMESPACE_END(index);


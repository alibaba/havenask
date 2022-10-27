#ifndef __INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_H
#define __INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/tiered_dictionary_writer.h"
#include "indexlib/storage/archive_folder.h"

DECLARE_REFERENCE_CLASS(config, IndexConfig);
DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(storage, ArchiveFolder);
DECLARE_REFERENCE_CLASS(index, AdaptiveBitmapTrigger);
DECLARE_REFERENCE_CLASS(index, PostingMerger);
DECLARE_REFERENCE_CLASS(index, PostingIterator);
DECLARE_REFERENCE_CLASS(index, BitmapPostingWriter);

IE_NAMESPACE_BEGIN(index);

class AdaptiveBitmapIndexWriter
{
public:
    AdaptiveBitmapIndexWriter(
            const AdaptiveBitmapTriggerPtr& trigger,
            const config::IndexConfigPtr& indexConf,
            const storage::ArchiveFolderPtr& adaptiveDictFolder);

    virtual ~AdaptiveBitmapIndexWriter();

public:
    void Init(const DictionaryWriterPtr& dictWriter,
              const file_system::FileWriterPtr& postingWriter);

    bool NeedAdaptiveBitmap(dictkey_t dictKey, 
                            const PostingMergerPtr& postingMerger);

    virtual void AddPosting(
            dictkey_t dictKey, termpayload_t termPayload,
            const PostingIteratorPtr& postingIt);

    void EndPosting();

    int64_t EstimateMemoryUse(uint32_t docCount) const;

public:
    // for test
    const std::vector<dictkey_t>& GetAdaptiveDictKeys()
    {  
        return mAdaptiveDictKeys;
    }

    // for test
    std::string GetAdaptiveDictDir() const { return mAdaptiveDictFolder->GetFolderPath(); }

protected:
    void CollectDocIds(const PostingIteratorPtr& postingIt,
                       std::vector<docid_t>& docIds);

    void DumpPosting(dictkey_t dictKey,
                     const BitmapPostingWriterPtr& writer);

    void DumpAdaptiveDictFile();
    
protected:
    typedef TieredDictionaryWriter<dictkey_t> BitmapDictionaryWriter;
    typedef std::tr1::shared_ptr<BitmapDictionaryWriter> BitmapDictionaryWriterPtr;

    AdaptiveBitmapTriggerPtr mAdaptiveBitmapTrigger;
    config::IndexConfigPtr mIndexConfig;
    storage::ArchiveFolderPtr mAdaptiveDictFolder;

    BitmapDictionaryWriterPtr mDictionaryWriter;
    file_system::FileWriterPtr mPostingWriter;

    std::vector<dictkey_t> mAdaptiveDictKeys;
private:
    friend class AdaptiveBitmapIndexWriterTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AdaptiveBitmapIndexWriter);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ADAPTIVE_BITMAP_INDEX_WRITER_H

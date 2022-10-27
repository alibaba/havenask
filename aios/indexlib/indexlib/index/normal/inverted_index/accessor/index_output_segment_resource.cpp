#include "indexlib/index/normal/inverted_index/accessor/index_output_segment_resource.h"
#include "indexlib/index/normal/inverted_index/format/dictionary/dictionary_creator.h"
#include "indexlib/config/index_config.h"


using namespace std;

IE_NAMESPACE_USE(file_system);
IE_NAMESPACE_USE(misc);
IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, IndexOutputSegmentResource);

IndexOutputSegmentResource::~IndexOutputSegmentResource()
{
    if (mNormalIndexDataWriter)
    {
        mNormalIndexDataWriter->Reset();
    }
    if (mBitmapIndexDataWriter)
    {
        mBitmapIndexDataWriter->Reset();
    }
}

void IndexOutputSegmentResource::Reset()
{
    DestroyIndexDataWriters();
}

void IndexOutputSegmentResource::Init(
        const DirectoryPtr& mergeDir, const config::IndexConfigPtr& indexConfig,
        const config::MergeIOConfig& ioConfig, util::SimplePool* simplePool,
        bool hasAdaptiveBitMap)
{
    mMergeDir = mergeDir;
    CreateNormalIndexDataWriter(indexConfig, ioConfig, simplePool);
    CreateBitmapIndexDataWriter(indexConfig, ioConfig, simplePool, hasAdaptiveBitMap);
}

void IndexOutputSegmentResource::CreateNormalIndexDataWriter(
        const config::IndexConfigPtr& indexConfig,
        const config::MergeIOConfig& IOConfig,
        util::SimplePool* simplePool)
{
    mNormalIndexDataWriter.reset(new IndexDataWriter());
    mNormalIndexDataWriter->dictWriter.reset(DictionaryCreator::CreateWriter(indexConfig, simplePool));
    if (mNormalIndexDataWriter->dictWriter)
    {
        if (mDictKeyCount > 0)
        {
            mNormalIndexDataWriter->dictWriter->Open(mMergeDir, DICTIONARY_FILE_NAME, mDictKeyCount);
        }
        else
        {
            mNormalIndexDataWriter->dictWriter->Open(mMergeDir, DICTIONARY_FILE_NAME);
        }
    }
    else
    {
        IE_LOG(ERROR, "create normal index data writer fail, path[%s]",
               mMergeDir->GetPath().c_str());
    }
    FSWriterParam fsWriterParam(IOConfig.writeBufferSize, IOConfig.enableAsyncWrite);
    FileWriterPtr postingFileWriter = mMergeDir->CreateFileWriter(POSTING_FILE_NAME, fsWriterParam);
    mNormalIndexDataWriter->postingWriter = postingFileWriter;
 }

void IndexOutputSegmentResource::CreateBitmapIndexDataWriter(
        const config::IndexConfigPtr& indexConfig,
        const config::MergeIOConfig& IOConfig,
        util::SimplePool* simplePool,
        bool hasAdaptiveBitMap)

{
    if (indexConfig->GetHighFreqVocabulary() || hasAdaptiveBitMap)
    {
        mBitmapIndexDataWriter.reset(new IndexDataWriter());
        mBitmapIndexDataWriter->dictWriter.reset(new TieredDictionaryWriter<dictkey_t>(simplePool));
        mBitmapIndexDataWriter->dictWriter->Open(mMergeDir, BITMAP_DICTIONARY_FILE_NAME);

        FSWriterParam fsWriterParam(IOConfig.writeBufferSize, IOConfig.enableAsyncWrite);
        FileWriterPtr postingFileWriter = mMergeDir->CreateFileWriter(BITMAP_POSTING_FILE_NAME, fsWriterParam);
        mBitmapIndexDataWriter->postingWriter = postingFileWriter;
    }
}

IndexDataWriterPtr& IndexOutputSegmentResource::GetIndexDataWriter(SegmentTermInfo::TermIndexMode mode)
{
    if (mode == SegmentTermInfo::TM_BITMAP)
    {
        return mBitmapIndexDataWriter;
    }
    return mNormalIndexDataWriter;
}


IE_NAMESPACE_END(index);



#include "indexlib/index/normal/inverted_index/format/doc_list_encoder.h"
#include "indexlib/common/numeric_compress/new_pfordelta_compressor.h"
#include "indexlib/common/numeric_compress/encoder_provider.h"
#include "indexlib/common/numeric_compress/vbyte_compressor.h"
#include "indexlib/index/normal/inverted_index/format/short_list_optimize_util.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/in_mem_pair_value_skip_list_reader.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/in_mem_tri_value_skip_list_reader.h"

using namespace std;
using namespace std::tr1;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(util);
IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(index);

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, DocListEncoder);

void DocListEncoder::AddPosition(int32_t fieldIdxInPack)
{
    if (mDocListFormatOption.HasFieldMap())
    {
        assert(fieldIdxInPack < 8);
        assert(fieldIdxInPack >= 0);
        mFieldMap |= (1 << fieldIdxInPack);
    }
    mCurrentTF++;
    mTotalTF++;
}

void DocListEncoder::EndDocument(docid_t docId, docpayload_t docPayload)
{
    AddDocument(docId, docPayload, mCurrentTF, mFieldMap);
    mDF++;

    if (mTfBitmapWriter)
    {
        //set to remember the first occ in this doc
        mTfBitmapWriter->Set(mTotalTF - mCurrentTF);
        mTfBitmapWriter->EndDocument(mDF, mTotalTF);
    }

    mCurrentTF = 0;
    mFieldMap = 0;
}

void DocListEncoder::Dump(const file_system::FileWriterPtr& file)
{
    Flush();
    uint32_t docSkipListSize = 0;
    if (mDocSkipListWriter)
    {
        docSkipListSize = mDocSkipListWriter->EstimateDumpSize();
    }

    uint32_t docListSize = mDocListBuffer.EstimateDumpSize();
    IE_LOG(TRACE1, "doc skip list length: [%u], doc list length: [%u",
           docSkipListSize, docListSize);

    file->WriteVUInt32(docSkipListSize);
    file->WriteVUInt32(docListSize);

    if (mDocSkipListWriter)
    {
        mDocSkipListWriter->Dump(file);
    }

    mDocListBuffer.Dump(file);
    if (mTfBitmapWriter)
    {
        mTfBitmapWriter->Dump(file, mTotalTF);
    }
}

void DocListEncoder::Flush()
{
    uint8_t compressMode = 
        ShortListOptimizeUtil::IsShortDocList(mDF) ? SHORT_LIST_COMPRESS_MODE : mCompressMode;
    FlushDocListBuffer(compressMode);
    if (mDocSkipListWriter)
    {
        mDocSkipListWriter->FinishFlush();
    }
    if (mTfBitmapWriter)
    {
        mTfBitmapWriter->Resize(mTotalTF);
    }
}

uint32_t DocListEncoder::GetDumpLength() const
{
    uint32_t docSkipListSize = 0;
    if (mDocSkipListWriter)
    {
        docSkipListSize = mDocSkipListWriter->EstimateDumpSize();
    }

    uint32_t docListSize = mDocListBuffer.EstimateDumpSize();
    uint32_t tfBitmapLength = 0;
    if (mTfBitmapWriter)
    {
        tfBitmapLength = mTfBitmapWriter->GetDumpLength(mTotalTF);
    }
    return VByteCompressor::GetVInt32Length(docSkipListSize) + 
        VByteCompressor::GetVInt32Length(docListSize) + 
        docSkipListSize + docListSize + tfBitmapLength;
}

void DocListEncoder::AddDocument(docid_t docId, docpayload_t docPayload, 
                                 tf_t tf, fieldmap_t fieldMap)
{
    if (!PostingFormatOption::IsReferenceCompress(mCompressMode)) 
    {
        mDocListBuffer.PushBack(0, docId - mLastDocId);
    }
    else
    {
        mDocListBuffer.PushBack(0, docId);
    }

    int n = 1;
    if (mDocListFormatOption.HasTfList())
    {
        mDocListBuffer.PushBack(n++, tf);
    }
    if (mDocListFormatOption.HasDocPayload())
    {
        mDocListBuffer.PushBack(n++, docPayload);
    }
    if (mDocListFormatOption.HasFieldMap())
    {
        mDocListBuffer.PushBack(n++, fieldMap);
    }

    mDocListBuffer.EndPushBack();

    mLastDocId = docId;
    mLastDocPayload = docPayload;
    mLastFieldMap = fieldMap;

    if (mDocListBuffer.NeedFlush())
    {
        FlushDocListBuffer(mCompressMode);
    }
}

void DocListEncoder::FlushDocListBuffer(uint8_t compressMode)
{
    uint32_t flushSize = mDocListBuffer.Flush(compressMode);
    if (flushSize > 0)
    {
        if (compressMode != SHORT_LIST_COMPRESS_MODE)
        {
            if (mDocSkipListWriter == NULL)
            {
                CreateDocSkipListWriter();                
            }
            AddSkipListItem(flushSize);
        }
    }
}

void DocListEncoder::CreateDocSkipListWriter()
{
    void *buffer = mByteSlicePool->allocate(sizeof(BufferedSkipListWriter));
    RecyclePool *bufferPool = dynamic_cast<RecyclePool*>(
            mDocListBuffer.GetBufferPool());
    assert(bufferPool);

    BufferedSkipListWriter* docSkipListWriter = 
        new(buffer)BufferedSkipListWriter(
                mByteSlicePool, bufferPool, mCompressMode);
    docSkipListWriter->Init(mDocListFormat->GetDocListSkipListFormat());
    // skip list writer should be atomic created;
    mDocSkipListWriter = docSkipListWriter;
}

void DocListEncoder::AddSkipListItem(uint32_t itemSize)
{
    const DocListSkipListFormat* skipListFormat = 
        mDocListFormat->GetDocListSkipListFormat();
    assert(skipListFormat);

    if (skipListFormat->HasTfList())
    {
        mDocSkipListWriter->AddItem(mLastDocId, mTotalTF, itemSize);
    }
    else
    {
        mDocSkipListWriter->AddItem(mLastDocId, itemSize);
    }
}

InMemDocListDecoder* DocListEncoder::GetInMemDocListDecoder(
        Pool *sessionPool) const
{
    // copy sequence
    // df -> skiplist -> doclist -> (TBD: poslist)
    df_t df = mDF;

    //TODO: delete buffer in pool
    SkipListReader* skipListReader = NULL;
    if (mDocSkipListWriter) 
    {
        const DocListSkipListFormat* skipListFormat = 
            mDocListFormat->GetDocListSkipListFormat();
        assert(skipListFormat);

        if (skipListFormat->HasTfList())
        {
            InMemTriValueSkipListReader* inMemSkipListReader
                = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
                        InMemTriValueSkipListReader, sessionPool);
            inMemSkipListReader->Load(mDocSkipListWriter);
            skipListReader = inMemSkipListReader;
        }
        else
        {
            InMemPairValueSkipListReader* inMemSkipListReader
                = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
                        InMemPairValueSkipListReader, sessionPool, 
                        mCompressMode == REFERENCE_COMPRESS_MODE);
            inMemSkipListReader->Load(mDocSkipListWriter);
            skipListReader = inMemSkipListReader;
        }
    }
    BufferedByteSlice* docListBuffer = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            BufferedByteSlice, sessionPool, sessionPool);
    mDocListBuffer.SnapShot(docListBuffer);

    InMemDocListDecoder* decoder = IE_POOL_COMPATIBLE_NEW_CLASS(sessionPool,
            InMemDocListDecoder, sessionPool,
            mCompressMode == REFERENCE_COMPRESS_MODE);
    decoder->Init(df, skipListReader, docListBuffer);

    return decoder;
}

IE_NAMESPACE_END(index);


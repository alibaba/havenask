#ifndef __INDEXLIB_DOC_LIST_ENCODER_H
#define __INDEXLIB_DOC_LIST_ENCODER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
#include <autil/mem_pool/Pool.h>
#include <autil/mem_pool/RecyclePool.h>
#include "indexlib/util/simple_pool.h"

#include "indexlib/index/normal/inverted_index/format/doc_list_format_option.h"
#include "indexlib/index/normal/inverted_index/accessor/position_bitmap_writer.h"
#include "indexlib/index/normal/inverted_index/format/buffered_byte_slice.h"
#include "indexlib/index/normal/inverted_index/format/in_mem_doc_list_decoder.h"
#include "indexlib/index/normal/inverted_index/format/doc_list_format.h"
#include "indexlib/index/normal/inverted_index/format/skiplist/buffered_skip_list_writer.h"
#include "indexlib/file_system/file_writer.h"

IE_NAMESPACE_BEGIN(index);

class DocListEncoder
{
public:
    DocListEncoder(const DocListFormatOption& docListFormatOption,
                   util::SimplePool* simplePool, 
                   autil::mem_pool::Pool* byteSlicePool, 
                   autil::mem_pool::RecyclePool* bufferPool,
                   DocListFormat* docListFormat = NULL,
                   index::CompressMode compressMode = index::PFOR_DELTA_COMPRESS_MODE)
        : mDocListBuffer(byteSlicePool, bufferPool)
        , mOwnDocListFormat(false)
        , mDocListFormatOption(docListFormatOption)
        , mFieldMap(0)
        , mCurrentTF(0)
        , mTotalTF(0)
        , mDF(0)
        , mLastDocId(0)
        , mLastDocPayload(0)
        , mLastFieldMap(0)
        , mCompressMode(compressMode)
        , mTfBitmapWriter(NULL)
        , mDocSkipListWriter(NULL)
        , mDocListFormat(docListFormat)
        , mByteSlicePool(byteSlicePool)
    {
        assert(byteSlicePool);
        assert(bufferPool);
        if (docListFormatOption.HasTfBitmap())
        {
            mTfBitmapWriter = IE_POOL_COMPATIBLE_NEW_CLASS(
                byteSlicePool, index::PositionBitmapWriter, simplePool);
        }
        // TODO, docListFormat can not be NULL, and should get Init return value
        if (!docListFormat)
        {
            mDocListFormat = new DocListFormat(docListFormatOption);
            mOwnDocListFormat = true;
        }
        mDocListBuffer.Init(mDocListFormat);
    }

    ~DocListEncoder()
    {
        if (mDocSkipListWriter)
        {
            mDocSkipListWriter->~BufferedSkipListWriter();
            mDocSkipListWriter = NULL;
        }
        if (mOwnDocListFormat)
        {
            DELETE_AND_SET_NULL(mDocListFormat);
        }
        IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool, mTfBitmapWriter);
        mTfBitmapWriter = NULL;
    }

public:
    void AddPosition(int32_t fieldIdxInPack);
    void EndDocument(docid_t docId, docpayload_t docPayload);
    void Dump(const file_system::FileWriterPtr& file);
    void Flush();
    uint32_t GetDumpLength() const;
    
public:
    uint32_t GetCurrentTF() const { return mCurrentTF; }
    uint32_t GetTotalTF() const  { return mTotalTF; }
    uint32_t GetDF() const { return mDF; }

    fieldmap_t GetFieldMap() const { return mFieldMap; }
    docid_t GetLastDocId() const { return mLastDocId; }
    docpayload_t GetLastDocPayload() const { return mLastDocPayload; }
    fieldmap_t GetLastFieldMap() const { return mLastFieldMap; }

    void SetFieldMap(fieldmap_t fieldMap) { mFieldMap = fieldMap; }
    void SetCurrentTF(tf_t tf)
    { mCurrentTF = tf; mTotalTF += tf; }

    InMemDocListDecoder* GetInMemDocListDecoder(
            autil::mem_pool::Pool *sessionPool) const;

    index::PositionBitmapWriter* GetPositionBitmapWriter() const
    {  return mTfBitmapWriter; }

public:
    //public for ut
    void SetDocSkipListWriter(index::BufferedSkipListWriter* skipListBuffer)
    { mDocSkipListWriter = skipListBuffer; }

    // for test
    void SetPositionBitmapWriter(index::PositionBitmapWriter* tfBitmapWriter)
    { 
        IE_POOL_COMPATIBLE_DELETE_CLASS(mByteSlicePool, mTfBitmapWriter);
        mTfBitmapWriter = tfBitmapWriter; 
    }

    void FlushDocListBuffer(uint8_t compressMode);

    BufferedByteSlice* GetDocListBuffer() { return &mDocListBuffer; }

private:
    void AddDocument(docid_t docId, docpayload_t docPayload,
                     tf_t tf, fieldmap_t fieldMap);
    void CreateDocSkipListWriter();
    void AddSkipListItem(uint32_t itemSize);

public:
    // for test
    const DocListFormat* GetDocListFormat() const
    { return mDocListFormat; }

private:
    BufferedByteSlice mDocListBuffer;
    bool mOwnDocListFormat;                   // 1byte
    DocListFormatOption mDocListFormatOption; // 1byte
    fieldmap_t mFieldMap;                     // 1byte
    tf_t mCurrentTF;                          // 4byte
    tf_t mTotalTF;                            // 4byte
    df_t volatile mDF;                        // 4byte

    docid_t mLastDocId;           // 4byte
    docpayload_t mLastDocPayload; // 2byte
    fieldmap_t mLastFieldMap;     // 1byte
    index::CompressMode mCompressMode; //1byte
    // volatile for realtime
    index::PositionBitmapWriter* mTfBitmapWriter; //8byte
    index::BufferedSkipListWriter* volatile mDocSkipListWriter;
    DocListFormat* mDocListFormat;
    autil::mem_pool::Pool* mByteSlicePool;

private:
    friend class DocListEncoderTest;
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(DocListEncoder);
IE_NAMESPACE_END(index);

#endif //__INDEXLIB_DOC_LIST_ENCODER_H

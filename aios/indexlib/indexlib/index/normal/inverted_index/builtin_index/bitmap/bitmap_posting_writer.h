#ifndef __INDEXLIB_BITMAP_POSTING_WRITER_H
#define __INDEXLIB_BITMAP_POSTING_WRITER_H

#include <tr1/memory>
#include "fslib/fs/File.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

#include "indexlib/util/expandable_bitmap.h"
#include "indexlib/common/byte_slice_writer.h"
#include "indexlib/index/normal/inverted_index/accessor/posting_writer.h"
#include <autil/mem_pool/PoolBase.h>

IE_NAMESPACE_BEGIN(index);

class BitmapPostingWriter : public PostingWriter
{
public:
    BitmapPostingWriter(autil::mem_pool::PoolBase* pool = NULL);
    ~BitmapPostingWriter();

public:
    
    void AddPosition(pos_t pos, pospayload_t posPayload, 
                     fieldid_t fieldId) override
    {
        mCurrentTF++;
        mTotalTF++;
    }

    void EndDocument(docid_t docId, docpayload_t docPayload) override;
    void EndDocument(docid_t docId, docpayload_t docPayload, fieldmap_t fieldMap) override
    { return EndDocument(docId, docPayload); }

    void EndSegment() override {}

    void Dump(const file_system::FileWriterPtr& file) override;

    uint32_t GetDumpLength() const override;

    uint32_t GetTotalTF() const override
    { return mTotalTF; }

    uint32_t GetDF() const override
    { return mDF; }

    bool NotExistInCurrentDoc() const override
    { return mCurrentTF == 0; }

    void SetTermPayload(termpayload_t payload) override
    { mTermPayload = payload; }

    termpayload_t GetTermPayload() const override
    { return mTermPayload; }

    docid_t GetLastDocId() const override
    { return mLastDocId; }

public:
    void AddPosition(pos_t pos, pospayload_t posPayload)
    { AddPosition(pos, posPayload, 0); }

    const util::ExpandableBitmap& GetBitmap() const 
    { return mBitmap; }

    const util::ExpandableBitmap* GetBitmapData() const 
    { return &mBitmap; }

private:
    uint32_t mDF;
    uint32_t mTotalTF;
    termpayload_t mTermPayload;
    util::ExpandableBitmap mBitmap;
    docid_t mLastDocId;
    tf_t mCurrentTF;

    static const uint32_t INIT_BITMAP_ITEM_NUM = 128 * 1024;
private:
    IE_LOG_DECLARE();
};

typedef std::tr1::shared_ptr<BitmapPostingWriter> BitmapPostingWriterPtr;

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_BITMAP_POSTING_WRITER_H

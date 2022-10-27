#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_writer.h"
#include "indexlib/index/normal/inverted_index/format/term_meta_dumper.h"
#include "indexlib/util/numeric_util.h"
#include "indexlib/index/normal/inverted_index/accessor/term_meta.h"

using namespace std;
using namespace fslib::fs;
using namespace autil::mem_pool;

IE_NAMESPACE_USE(common);
IE_NAMESPACE_USE(util);


IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BitmapPostingWriter);

BitmapPostingWriter::BitmapPostingWriter(PoolBase* pool) 
    : mDF(0)
    , mTotalTF(0)
    , mTermPayload(0)
    , mBitmap(INIT_BITMAP_ITEM_NUM, false, pool)
    , mCurrentTF(0)
{
}

BitmapPostingWriter::~BitmapPostingWriter() 
{
}

void BitmapPostingWriter::EndDocument(docid_t docId, docpayload_t docPayload)
{
    mDF++;
    mCurrentTF = 0;
    mBitmap.Set(docId);
    mLastDocId = docId;
}

void BitmapPostingWriter::Dump(const file_system::FileWriterPtr& file)
{
    TermMeta termMeta(GetDF(), GetTotalTF(), GetTermPayload());
    TermMetaDumper tmDumper;
    tmDumper.Dump(file, termMeta);

    uint32_t size = NumericUtil::UpperPack(mLastDocId + 1, Bitmap::SLOT_SIZE);
    size = size / Bitmap::BYTE_SLOT_NUM;
    file->Write((void*)&size, sizeof(uint32_t));
    file->Write((void*)mBitmap.GetData(), size);
}

uint32_t BitmapPostingWriter::GetDumpLength() const
{
    TermMeta termMeta(GetDF(), GetTotalTF(), GetTermPayload());
    TermMetaDumper tmDumper;
    
    uint32_t size = NumericUtil::UpperPack(mLastDocId + 1, Bitmap::SLOT_SIZE);
    size = size / Bitmap::BYTE_SLOT_NUM;
    return size + sizeof(uint32_t) + tmDumper.CalculateStoreSize(termMeta);
}

IE_NAMESPACE_END(index);


#include "indexlib/index/normal/inverted_index/builtin_index/bitmap/bitmap_posting_dumper.h"

using namespace std;

IE_NAMESPACE_BEGIN(index);
IE_LOG_SETUP(index, BitmapPostingDumper);

//BitmapPostingDumper::BitmapPostingDumper() { }

BitmapPostingDumper::~BitmapPostingDumper() { }

void BitmapPostingDumper::Dump(const file_system::FileWriterPtr& postingFile) {
    mPostingWriter->SetTermPayload(mTermPayload);
    uint32_t totalLength = mPostingWriter->GetDumpLength();
    postingFile->Write((void*)&totalLength, sizeof(uint32_t));
    mPostingWriter->Dump(postingFile);
}

uint64_t BitmapPostingDumper::GetDumpLength() const  
{ 
    return sizeof(uint32_t) + mPostingWriter->GetDumpLength(); 
}


IE_NAMESPACE_END(index);


#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_PATCH_MERGER_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_PATCH_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"

#include "indexlib/common_define.h"
// #include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"
#include "indexlib/index_define.h"
#include "indexlib/util/mem_buffer.h"
#include "indexlib/file_system/file_writer.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class VarNumAttributePatchMerger : public AttributePatchMerger
{
public:
    typedef std::tr1::shared_ptr<VarNumAttributePatchReader<T> > PatchReaderPtr;

public:
    VarNumAttributePatchMerger(
            const config::AttributeConfigPtr& attrConfig,
            const SegmentUpdateBitmapPtr& segUpdateBitmap = SegmentUpdateBitmapPtr())
        : AttributePatchMerger(attrConfig, segUpdateBitmap)
    {}
    
    virtual ~VarNumAttributePatchMerger() {}

public:
    void Merge(const index_base::AttrPatchFileInfoVec& patchFileInfoVec,
               const file_system::FileWriterPtr& destPatchFile) override;

private:
    void DoMerge(const PatchReaderPtr& patchReader,
                 const file_system::FileWriterPtr& destPatchFile);
private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumAttributePatchMerger);
//////////////////////////////////////////////////////////////////////

template<typename T>
inline void VarNumAttributePatchMerger<T>::Merge(
        const index_base::AttrPatchFileInfoVec& patchFileInfoVec,
        const file_system::FileWriterPtr& destPatchFile)
{
    if (patchFileInfoVec.size() == 1 && !mSegUpdateBitmap)
    {
        CopyFile(patchFileInfoVec[0].patchDirectory,
                 patchFileInfoVec[0].patchFileName,
                 destPatchFile);
        return;
    }
    PatchReaderPtr patchReader(new VarNumAttributePatchReader<T>(mAttrConfig));
    for (size_t i = 0; i < patchFileInfoVec.size(); i++)
    {
        patchReader->AddPatchFile(patchFileInfoVec[i].patchDirectory, 
                patchFileInfoVec[i].patchFileName, 
                patchFileInfoVec[i].srcSegment);
    }
    DoMerge(patchReader, destPatchFile);
}

template<typename T>
void VarNumAttributePatchMerger<T>::DoMerge(const PatchReaderPtr& patchReader,
                                            const file_system::FileWriterPtr& destPatchFile)
{
    file_system::FileWriterPtr patchFile = CreatePatchFileWriter(destPatchFile);
    assert(patchFile);
    uint32_t patchCount = 0;
    uint32_t maxPatchLen = 0;

    docid_t docId = INVALID_DOCID;
    util::MemBuffer buff;
    buff.Reserve(patchReader->GetMaxPatchItemLen());
    size_t dataLen = patchReader->Next(docId, 
            (uint8_t*)buff.GetBuffer(), buff.GetBufferSize());
    while(dataLen > 0)
    {
        ++patchCount;
        maxPatchLen = std::max(maxPatchLen, (uint32_t)dataLen);
        patchFile->Write((void*)&docId, sizeof(docid_t));
        patchFile->Write((void*)buff.GetBuffer(), dataLen);
        if (mSegUpdateBitmap)
        {
            mSegUpdateBitmap->Set(docId);
        }
        dataLen = patchReader->Next(docId, 
                (uint8_t*)buff.GetBuffer(), buff.GetBufferSize());
    }
    patchFile->Write(&patchCount, sizeof(uint32_t));
    patchFile->Write(&maxPatchLen, sizeof(uint32_t));
    patchFile->Close();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_PATCH_MERGER_H

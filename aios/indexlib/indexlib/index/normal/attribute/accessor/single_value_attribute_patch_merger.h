#ifndef __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_PATCH_MERGER_H
#define __INDEXLIB_SINGLE_VALUE_ATTRIBUTE_PATCH_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
// #include "indexlib/storage/file_system_wrapper.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_patch_iterator.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"
#include "indexlib/file_system/file_writer.h"

IE_NAMESPACE_BEGIN(index);

template<typename T>
class SingleValueAttributePatchMerger : public AttributePatchMerger
{
private:
    typedef SingleValueAttributeSegmentPatchIterator<T> PatchIterator;
public:
    SingleValueAttributePatchMerger(
            const config::AttributeConfigPtr& attrConfig,
            const SegmentUpdateBitmapPtr& segUpdateBitmap = SegmentUpdateBitmapPtr())
        : AttributePatchMerger(attrConfig, segUpdateBitmap)
    {}
    
    virtual ~SingleValueAttributePatchMerger() {}
public:
    void Merge(const index_base::AttrPatchFileInfoVec& patchFileInfoVec,
               const file_system::FileWriterPtr& destPatchFile) override;
    
private:
    void DoMerge(PatchIterator& patchIter,
                 const file_system::FileWriterPtr& destPatchFile);

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributePatchMerger);
//////////////////////////////////////////////////////////////////////

template<typename T>
void SingleValueAttributePatchMerger<T>::Merge(
        const index_base::AttrPatchFileInfoVec& patchFileInfoVec,
        const file_system::FileWriterPtr& destPatchFile)
{
    // TODO: check
    if (patchFileInfoVec.size() == 1 && !mSegUpdateBitmap)
    {
        CopyFile(patchFileInfoVec[0].patchDirectory,
                 patchFileInfoVec[0].patchFileName,
                 destPatchFile);
        return;
    }

    SingleValueAttributeSegmentPatchIterator<T> patchIter(mAttrConfig);
    for (size_t i = 0; i < patchFileInfoVec.size(); i++)
    {
        patchIter.AddPatchFile(patchFileInfoVec[i].patchDirectory, 
                               patchFileInfoVec[i].patchFileName, 
                               patchFileInfoVec[i].srcSegment);
    }
    DoMerge(patchIter, destPatchFile);
}

template<typename T>
void SingleValueAttributePatchMerger<T>::DoMerge(
        SingleValueAttributeSegmentPatchIterator<T>& patchIter,
        const file_system::FileWriterPtr& destPatchFile)
{
    file_system::FileWriterPtr dataFileWriter = CreatePatchFileWriter(destPatchFile);
    assert(dataFileWriter);
    docid_t docId;
    T value;
    while(patchIter.Next(docId, value))
    {
        dataFileWriter->Write((void*)&docId, sizeof(docid_t));
        dataFileWriter->Write((void*)&value, sizeof(T));
        if (mSegUpdateBitmap)
        {
            mSegUpdateBitmap->Set(docId);
        }
    }
    dataFileWriter->Close();
}

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_SINGLE_VALUE_ATTRIBUTE_PATCH_MERGER_H

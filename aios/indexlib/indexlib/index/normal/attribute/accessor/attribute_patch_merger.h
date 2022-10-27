#ifndef __INDEXLIB_ATTRIBUTE_PATCH_MERGER_H
#define __INDEXLIB_ATTRIBUTE_PATCH_MERGER_H

#include <tr1/memory>
#include "indexlib/indexlib.h"
#include "indexlib/common_define.h"
#include "indexlib/index_base/patch/patch_file_finder.h"
#include "indexlib/index/normal/attribute/segment_update_bitmap.h"

DECLARE_REFERENCE_CLASS(file_system, FileWriter);
DECLARE_REFERENCE_CLASS(config, AttributeConfig);

IE_NAMESPACE_BEGIN(index);

class AttributePatchMerger
{
public:
    typedef std::vector<std::pair<std::string, segmentid_t> > PatchFileList;

public:
    AttributePatchMerger(
            const config::AttributeConfigPtr& attrConfig,
            const SegmentUpdateBitmapPtr& segUpdateBitmap = SegmentUpdateBitmapPtr());
    
    virtual ~AttributePatchMerger();

public:
    virtual void Merge(const index_base::AttrPatchFileInfoVec& patchFileInfoVec,
                       const file_system::FileWriterPtr& destPatchFile) = 0;

protected:
    static void CopyFile(const file_system::DirectoryPtr& directory,
                         const std::string& srcFileName,
                         const file_system::FileWriterPtr& dstFileWriter);

    file_system::FileWriterPtr CreatePatchFileWriter(
        const file_system::FileWriterPtr& destPatchFile) const;

    
protected:
    config::AttributeConfigPtr mAttrConfig;
    SegmentUpdateBitmapPtr mSegUpdateBitmap;
    
private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(AttributePatchMerger);

IE_NAMESPACE_END(index);

#endif //__INDEXLIB_ATTRIBUTE_PATCH_MERGER_H

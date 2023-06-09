/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __INDEXLIB_VAR_NUM_ATTRIBUTE_PATCH_MERGER_H
#define __INDEXLIB_VAR_NUM_ATTRIBUTE_PATCH_MERGER_H

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
// #include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/accessor/var_num_attribute_patch_reader.h"
#include "indexlib/index_define.h"
#include "indexlib/util/MemBuffer.h"

namespace indexlib { namespace index {

template <typename T>
class VarNumAttributePatchMerger : public AttributePatchMerger
{
public:
    typedef std::shared_ptr<VarNumAttributePatchReader<T>> PatchReaderPtr;

public:
    VarNumAttributePatchMerger(const config::AttributeConfigPtr& attrConfig,
                               const SegmentUpdateBitmapPtr& segUpdateBitmap = SegmentUpdateBitmapPtr())
        : AttributePatchMerger(attrConfig, segUpdateBitmap)
    {
    }

    virtual ~VarNumAttributePatchMerger() {}

public:
    void Merge(const index_base::PatchFileInfoVec& patchFileInfoVec,
               const file_system::FileWriterPtr& destPatchFile) override;

private:
    void DoMerge(const PatchReaderPtr& patchReader, const file_system::FileWriterPtr& destPatchFile);

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, VarNumAttributePatchMerger);
//////////////////////////////////////////////////////////////////////

template <typename T>
inline void VarNumAttributePatchMerger<T>::Merge(const index_base::PatchFileInfoVec& patchFileInfoVec,
                                                 const file_system::FileWriterPtr& destPatchFile)
{
    if (patchFileInfoVec.size() == 1 && !mSegUpdateBitmap) {
        CopyFile(patchFileInfoVec[0].patchDirectory, patchFileInfoVec[0].patchFileName, destPatchFile);
        return;
    }
    PatchReaderPtr patchReader(new VarNumAttributePatchReader<T>(mAttrConfig));
    for (size_t i = 0; i < patchFileInfoVec.size(); i++) {
        patchReader->AddPatchFile(patchFileInfoVec[i].patchDirectory, patchFileInfoVec[i].patchFileName,
                                  patchFileInfoVec[i].srcSegment);
    }
    DoMerge(patchReader, destPatchFile);
}

template <typename T>
void VarNumAttributePatchMerger<T>::DoMerge(const PatchReaderPtr& patchReader,
                                            const file_system::FileWriterPtr& destPatchFile)
{
    file_system::FileWriterPtr patchFile =
        CreatePatchFileWriter(destPatchFile, mAttrConfig->GetCompressType().HasPatchCompress());
    assert(patchFile);
    uint32_t patchCount = 0;
    uint32_t maxPatchLen = 0;

    docid_t docId = INVALID_DOCID;
    util::MemBuffer buff;
    buff.Reserve(patchReader->GetMaxPatchItemLen());
    bool isNull = false;
    size_t dataLen = patchReader->Next(docId, (uint8_t*)buff.GetBuffer(), buff.GetBufferSize(), isNull);
    while (dataLen > 0) {
        ++patchCount;
        maxPatchLen = std::max(maxPatchLen, (uint32_t)dataLen);
        patchFile->Write((void*)&docId, sizeof(docid_t)).GetOrThrow();
        patchFile->Write((void*)buff.GetBuffer(), dataLen).GetOrThrow();
        if (mSegUpdateBitmap) {
            mSegUpdateBitmap->Set(docId);
        }
        dataLen = patchReader->Next(docId, (uint8_t*)buff.GetBuffer(), buff.GetBufferSize(), isNull);
    }
    patchFile->Write(&patchCount, sizeof(uint32_t)).GetOrThrow();
    patchFile->Write(&maxPatchLen, sizeof(uint32_t)).GetOrThrow();
    patchFile->Close().GetOrThrow();
}
}} // namespace indexlib::index

#endif //__INDEXLIB_VAR_NUM_ATTRIBUTE_PATCH_MERGER_H

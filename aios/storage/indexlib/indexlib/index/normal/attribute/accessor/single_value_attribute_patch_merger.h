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
#pragma once

#include <memory>

#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"
// #include "indexlib/file_system/fslib/FslibWrapper.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/normal/attribute/accessor/attribute_patch_merger.h"
#include "indexlib/index/normal/attribute/accessor/single_value_attribute_segment_patch_iterator.h"
#include "indexlib/index/normal/attribute/format/single_value_attribute_patch_formatter.h"

namespace indexlib { namespace index {

template <typename T>
class SingleValueAttributePatchMerger : public AttributePatchMerger
{
private:
    typedef SingleValueAttributeSegmentPatchIterator<T> PatchIterator;

public:
    SingleValueAttributePatchMerger(const config::AttributeConfigPtr& attrConfig,
                                    const SegmentUpdateBitmapPtr& segUpdateBitmap = SegmentUpdateBitmapPtr())
        : AttributePatchMerger(attrConfig, segUpdateBitmap)
    {
    }

    virtual ~SingleValueAttributePatchMerger() {}

public:
    void Merge(const index_base::PatchFileInfoVec& patchFileInfoVec,
               const file_system::FileWriterPtr& destPatchFile) override;

private:
    void DoMerge(PatchIterator& patchIter, const file_system::FileWriterPtr& destPatchFile);

private:
    IE_LOG_DECLARE();
};

IE_LOG_SETUP_TEMPLATE(index, SingleValueAttributePatchMerger);
//////////////////////////////////////////////////////////////////////

template <typename T>
void SingleValueAttributePatchMerger<T>::Merge(const index_base::PatchFileInfoVec& patchFileInfoVec,
                                               const file_system::FileWriterPtr& destPatchFile)
{
    // TODO: check
    if (patchFileInfoVec.size() == 1 && !mSegUpdateBitmap) {
        CopyFile(patchFileInfoVec[0].patchDirectory, patchFileInfoVec[0].patchFileName, destPatchFile);
        return;
    }

    SingleValueAttributeSegmentPatchIterator<T> patchIter(mAttrConfig);
    for (size_t i = 0; i < patchFileInfoVec.size(); i++) {
        patchIter.AddPatchFile(patchFileInfoVec[i].patchDirectory, patchFileInfoVec[i].patchFileName,
                               patchFileInfoVec[i].srcSegment);
    }
    DoMerge(patchIter, destPatchFile);
}

template <typename T>
void SingleValueAttributePatchMerger<T>::DoMerge(SingleValueAttributeSegmentPatchIterator<T>& patchIter,
                                                 const file_system::FileWriterPtr& destPatchFile)
{
    file_system::FileWriterPtr dataFileWriter =
        CreatePatchFileWriter(destPatchFile, mAttrConfig->GetCompressType().HasPatchCompress());
    SingleValueAttributePatchFormatter formatter;
    assert(mAttrConfig);
    assert(mAttrConfig->GetFieldConfig());
    bool supportNull = mAttrConfig->GetFieldConfig()->IsEnableNullField();
    formatter.InitForWrite(supportNull, dataFileWriter);

    assert(dataFileWriter);
    docid_t docId;
    T value {};
    bool isNull = false;
    while (patchIter.Next(docId, value, isNull)) {
        docid_t originDocId = docId;
        if (supportNull && isNull) {
            SingleValueAttributePatchFormatter::EncodedDocId(docId);
        }
        formatter.Write(docId, (uint8_t*)&value, sizeof(T));
        if (mSegUpdateBitmap) {
            mSegUpdateBitmap->Set(originDocId);
        }
    }
    formatter.Close();
}
}} // namespace indexlib::index

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

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "indexlib/config/CompressTypeOption.h"
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/attribute/SegmentUpdateBitmap.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/patch/AttributePatchMerger.h"
#include "indexlib/index/attribute/patch/MultiValueAttributePatchReader.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"
#include "indexlib/util/MemBuffer.h"

namespace indexlibv2::index {

template <typename T>
class MultiValueAttributePatchMerger : public AttributePatchMerger
{
public:
    MultiValueAttributePatchMerger(const std::shared_ptr<AttributeConfig>& attrConfig,
                                   const std::shared_ptr<SegmentUpdateBitmap>& segmentUpdateBitmap)
        : AttributePatchMerger(attrConfig, segmentUpdateBitmap)
    {
    }

    virtual ~MultiValueAttributePatchMerger() = default;

public:
    Status Merge(const PatchFileInfos& patchFileInfos,
                 const std::shared_ptr<indexlib::file_system::FileWriter>& destPatchFileWriter) override;

private:
    Status DoMerge(const std::shared_ptr<MultiValueAttributePatchReader<T>>& patchReader,
                   const std::shared_ptr<indexlib::file_system::FileWriter>& destPatchFileWriter);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, MultiValueAttributePatchMerger, T);

template <typename T>
inline Status
MultiValueAttributePatchMerger<T>::Merge(const PatchFileInfos& patchFileInfos,
                                         const std::shared_ptr<indexlib::file_system::FileWriter>& destPatchFile)
{
    if (patchFileInfos.Size() == 1 && !_segUpdateBitmap) {
        auto st = CopyFile(patchFileInfos[0].patchDirectory, patchFileInfos[0].patchFileName, destPatchFile);
        RETURN_IF_STATUS_ERROR(st, "copy patch file failed, file:%s", patchFileInfos[0].patchFileName.c_str());
        return Status::OK();
    }
    auto patchReader = std::make_shared<MultiValueAttributePatchReader<T>>(_attrConfig);
    for (size_t i = 0; i < patchFileInfos.Size(); i++) {
        auto st = patchReader->AddPatchFile(patchFileInfos[i].patchDirectory, patchFileInfos[i].patchFileName,
                                            patchFileInfos[i].srcSegment);
        RETURN_IF_STATUS_ERROR(st, "add patch file failed, file:%s", patchFileInfos[0].patchFileName.c_str());
    }
    auto st = DoMerge(patchReader, destPatchFile);
    RETURN_IF_STATUS_ERROR(st, "do merge patch file failed");
    return Status::OK();
}

template <typename T>
Status MultiValueAttributePatchMerger<T>::DoMerge(
    const std::shared_ptr<MultiValueAttributePatchReader<T>>& patchReader,
    const std::shared_ptr<indexlib::file_system::FileWriter>& destPatchFileWriter)
{
    std::shared_ptr<indexlib::file_system::FileWriter> patchFile =
        ConvertToCompressFileWriter(destPatchFileWriter, _attrConfig->GetCompressType().HasPatchCompress());
    assert(patchFile);
    uint32_t patchCount = 0;
    uint32_t maxPatchLen = 0;

    docid_t docId = INVALID_DOCID;
    indexlib::util::MemBuffer buff;
    buff.Reserve(patchReader->GetMaxPatchItemLen());
    bool isNull = false;
    size_t writeLen = 0;
    auto status = Status::OK();
    auto [st, dataLen] = patchReader->Next(docId, (uint8_t*)buff.GetBuffer(), buff.GetBufferSize(), isNull);
    RETURN_IF_STATUS_ERROR(st, "invalid next operstor for patch reader, docId[%d].", docId);
    while (dataLen > 0) {
        ++patchCount;
        maxPatchLen = std::max(maxPatchLen, (uint32_t)dataLen);
        std::tie(status, writeLen) = patchFile->Write((void*)&docId, sizeof(docid_t)).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "write doc id failed, docId[%d].", docId);
        std::tie(status, writeLen) = patchFile->Write((void*)buff.GetBuffer(), dataLen).StatusWith();
        RETURN_IF_STATUS_ERROR(status, "write patch value failed, docId[%d].", docId);
        if (_segUpdateBitmap) {
            status = _segUpdateBitmap->Set(docId);
            RETURN_IF_STATUS_ERROR(status, "set segment update bitmap failed, docId[%d]", docId);
        }
        std::tie(status, dataLen) = patchReader->Next(docId, (uint8_t*)buff.GetBuffer(), buff.GetBufferSize(), isNull);
        RETURN_IF_STATUS_ERROR(status, "invalid next operator for patch reader, docId[%d]", docId);
    }
    std::tie(status, writeLen) = patchFile->Write(&patchCount, sizeof(uint32_t)).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "write patch count failed, count[%u].", patchCount);
    std::tie(status, writeLen) = patchFile->Write(&maxPatchLen, sizeof(uint32_t)).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "write max patch len failed, count[%u].", maxPatchLen);
    status = patchFile->Close().Status();
    RETURN_IF_STATUS_ERROR(status, "close patch file failed.");
    return Status::OK();
}

} // namespace indexlibv2::index

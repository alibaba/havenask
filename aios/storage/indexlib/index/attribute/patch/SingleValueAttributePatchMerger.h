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
#include "indexlib/file_system/file/FileWriter.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"
#include "indexlib/index/attribute/format/SingleValueAttributePatchFormatter.h"
#include "indexlib/index/attribute/patch/AttributePatchMerger.h"
#include "indexlib/index/attribute/patch/SingleValueAttributePatchReader.h"
#include "indexlib/index/common/patch/PatchFileInfos.h"

namespace indexlibv2::index {

template <typename T>
class SingleValueAttributePatchMerger : public AttributePatchMerger
{
public:
    SingleValueAttributePatchMerger(const std::shared_ptr<AttributeConfig>& attrConfig,
                                    const std::shared_ptr<SegmentUpdateBitmap>& segmentUpdateBitmap)
        : AttributePatchMerger(attrConfig, segmentUpdateBitmap)
    {
    }

    virtual ~SingleValueAttributePatchMerger() = default;

public:
    Status Merge(const PatchFileInfos& patchFileInfos,
                 const std::shared_ptr<indexlib::file_system::FileWriter>& destPatchFileWriter) override;

private:
    Status DoMerge(const std::shared_ptr<SingleValueAttributePatchReader<T>>& patchReader,
                   const std::shared_ptr<indexlib::file_system::FileWriter>& destPatchFileWriter);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP_TEMPLATE(indexlib.index, SingleValueAttributePatchMerger, T);

template <typename T>
inline Status
SingleValueAttributePatchMerger<T>::Merge(const PatchFileInfos& patchFileInfos,
                                          const std::shared_ptr<indexlib::file_system::FileWriter>& destPatchFile)
{
    if (patchFileInfos.Size() == 1 && !_segUpdateBitmap) {
        auto st = CopyFile(patchFileInfos[0].patchDirectory, patchFileInfos[0].patchFileName, destPatchFile);
        RETURN_IF_STATUS_ERROR(st, "copy patch file failed, file:%s", patchFileInfos[0].patchFileName.c_str());
        return Status::OK();
    }
    auto patchReader = std::make_shared<SingleValueAttributePatchReader<T>>(_attrConfig);
    for (size_t i = 0; i < patchFileInfos.Size(); i++) {
        auto st = patchReader->AddPatchFile(patchFileInfos[i].patchDirectory, patchFileInfos[i].patchFileName,
                                            patchFileInfos[i].srcSegment);
        RETURN_IF_STATUS_ERROR(st, "add patch file failed, file:%s", patchFileInfos[0].patchFileName.c_str());
    }
    auto st = DoMerge(patchReader, destPatchFile);
    RETURN_IF_STATUS_ERROR(st, "do merge patch file failed, err[%s]", st.ToString().c_str());
    return Status::OK();
}

template <typename T>
Status SingleValueAttributePatchMerger<T>::DoMerge(
    const std::shared_ptr<SingleValueAttributePatchReader<T>>& patchReader,
    const std::shared_ptr<indexlib::file_system::FileWriter>& destPatchFileWriter)
{
    std::shared_ptr<indexlib::file_system::FileWriter> patchFile =
        ConvertToCompressFileWriter(destPatchFileWriter, _attrConfig->GetCompressType().HasPatchCompress());
    assert(patchFile);
    SingleValueAttributePatchFormatter formatter;
    assert(_attrConfig->GetFieldConfig());
    bool supportNull = _attrConfig->GetFieldConfig()->IsEnableNullField();
    formatter.InitForWrite(supportNull, patchFile);

    docid_t docId;
    T value {};
    bool isNull = false;
    auto [st, sucess] = patchReader->Next(docId, value, isNull);
    RETURN_IF_STATUS_ERROR(st, "invalid next operator for patch reader.");
    while (sucess) {
        docid_t originDocId = docId;
        if (supportNull && isNull) {
            SingleValueAttributePatchFormatter::EncodedDocId(docId);
        }
        st = formatter.Write(docId, (uint8_t*)&value, sizeof(T));
        RETURN_IF_STATUS_ERROR(st, "write doc id failed, docId[%d].", docId);
        if (_segUpdateBitmap) {
            st = _segUpdateBitmap->Set(originDocId);
            RETURN_IF_STATUS_ERROR(st, "set segment update bitmap failed, docId[%d]", originDocId);
        }
        std::tie(st, sucess) = patchReader->Next(docId, value, isNull);
        RETURN_IF_STATUS_ERROR(st, "invalid next operator for patch reader.");
    }

    st = formatter.Close();
    RETURN_IF_STATUS_ERROR(st, "close single value patch file failed.");

    return Status::OK();
}

} // namespace indexlibv2::index

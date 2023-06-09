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
#include "indexlib/index/normal/attribute/accessor/pack_attribute_merger.h"

#include "indexlib/common/field_format/attribute/attribute_convertor_factory.h"
#include "indexlib/common/field_format/pack_attribute/pack_attribute_formatter.h"
#include "indexlib/index/normal/attribute/accessor/dedup_pack_attribute_patch_file_merger.h"
#include "indexlib/index/normal/attribute/accessor/pack_attribute_patch_reader.h"

using namespace std;
using namespace autil;
using namespace indexlib::common;
using namespace indexlib::config;
using namespace indexlib::index_base;

namespace indexlib { namespace index {
IE_LOG_SETUP(index, PackAttributeMerger);

PackAttributeMerger::PackAttributeMerger(bool needMergePatch) : VarNumAttributeMerger<char>(needMergePatch) {}

PackAttributeMerger::~PackAttributeMerger() {}

void PackAttributeMerger::Init(const PackAttributeConfigPtr& packAttrConf, const MergeItemHint& hint,
                               const MergeTaskResourceVector& taskResources)
{
    VarNumAttributeMerger<char>::Init(packAttrConf->CreateAttributeConfig(), hint, taskResources);
    mPackAttrConfig = packAttrConf;
    mPackAttrFormatter.reset(new PackAttributeFormatter());
    mPackAttrFormatter->Init(packAttrConf);

    AttributeConfigPtr packDataAttrConfig = mPackAttrConfig->CreateAttributeConfig();
    mDataConvertor.reset(AttributeConvertorFactory::GetInstance()->CreateAttrConvertor(packDataAttrConfig));
    assert(mDataConvertor);
}

AttributePatchReaderPtr PackAttributeMerger::CreatePatchReaderForSegment(segmentid_t segmentId)
{
    PackAttributePatchReaderPtr patchReader(new PackAttributePatchReader(mPackAttrConfig));
    patchReader->Init(mSegmentDirectory->GetPartitionData(), segmentId);
    return patchReader;
}

int64_t PackAttributeMerger::EstimateMemoryUse(const SegmentDirectoryBasePtr& segDir, const MergerResource& resource,
                                               const index_base::SegmentMergeInfos& segMergeInfos,
                                               const index_base::OutputSegmentMergeInfos& outputSegMergeInfos,
                                               bool isSortedMerge) const
{
    DedupPackAttributePatchFileMerger patchMerger(mPackAttrConfig);

    // TODO: reserve buffer
    return VarNumAttributeMerger<char>::EstimateMemoryUse(segDir, resource, segMergeInfos, outputSegMergeInfos,
                                                          isSortedMerge) +
           GetMaxDataItemLen(segDir, segMergeInfos) * 3 // mergerBuffer(2) + worst patchBuffer(1)
           + patchMerger.EstimateMemoryUse(segDir->GetPartitionData(), segMergeInfos);
}

uint32_t PackAttributeMerger::ReadData(docid_t docId, const SegmentReaderWithCtx& segReader,
                                       const AttributePatchReaderPtr& patchReader, uint8_t* dataBuf,
                                       uint32_t dataBufLen)
{
    uint32_t patchDataLen, dataLen;
    patchDataLen = patchReader->Seek(docId, (uint8_t*)mPatchBuffer.GetBuffer(), mPatchBuffer.GetBufferSize());
    EnsureReadCtx();
    if (!segReader.reader->ReadDataAndLen(docId, segReader.ctx, dataBuf, dataBufLen, dataLen)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "read attribute data for doc [%d] failed.", docId);
    }

    if (patchDataLen == 0) {
        return dataLen;
    }
    PackAttributeFormatter::PackAttributeFields patchFields;
    if (!PackAttributeFormatter::DecodePatchValues((uint8_t*)mPatchBuffer.GetBuffer(), patchDataLen, patchFields)) {
        INDEXLIB_FATAL_ERROR(IndexCollapsed, "decode patch value for doc [%d] failed.", docId);
    }

    // skip count header(length)
    size_t encodeCountLen = 0;
    bool isNull = false;
    VarNumAttributeFormatter::DecodeCount((const char*)dataBuf, encodeCountLen, isNull);
    assert(!isNull);
    const char* pData = (const char*)dataBuf + encodeCountLen;
    StringView mergedValue =
        mPackAttrFormatter->MergeAndFormatUpdateFields((const char*)pData, patchFields, false, mMergeBuffer);
    if (mergedValue.empty()) {
        IE_LOG(ERROR,
               "merge patch value for doc [%d] failed."
               "ReadData without patch.",
               docId);
        return dataLen;
    }

    // remove hashKey
    common::AttrValueMeta decodeMeta = mDataConvertor->Decode(mergedValue);
    StringView decodeValue = decodeMeta.data;

    assert(decodeValue.length() <= dataBufLen);
    memcpy(dataBuf, decodeValue.data(), decodeValue.length());
    return decodeValue.length();
}

void PackAttributeMerger::MergePatches(const MergerResource& resource,
                                       const index_base::SegmentMergeInfos& segMergeInfos,
                                       const index_base::OutputSegmentMergeInfos& outputSegMergeInfos)
{
    DoMergePatches<DedupPackAttributePatchFileMerger>(resource, segMergeInfos, outputSegMergeInfos, mPackAttrConfig);
}

void PackAttributeMerger::ReserveMemBuffers(const vector<AttributePatchReaderPtr>& patchReaders,
                                            const vector<SegmentReaderWithCtx>& segReaders)
{
    uint32_t maxPatchItemLen = 0;
    for (size_t i = 0; i < patchReaders.size(); i++) {
        maxPatchItemLen = std::max(patchReaders[i]->GetMaxPatchItemLen(), maxPatchItemLen);
    }
    mPatchBuffer.Reserve(maxPatchItemLen);

    uint32_t maxDataItemLen = 0;
    for (size_t i = 0; i < segReaders.size(); i++) {
        auto dfsSegReader = DYNAMIC_POINTER_CAST(MultiValueAttributeSegmentReader<char>, segReaders[i].reader);
        if (dfsSegReader) {
            assert(!dfsSegReader->GetBaseAddress());
            maxDataItemLen = std::max(dfsSegReader->GetMaxDataItemLen(), maxDataItemLen);
        }
    }
    mMergeBuffer.Reserve(maxDataItemLen * 2);
    mDataBuf.Reserve(maxPatchItemLen + maxDataItemLen);
    // copy mergeValue to dataBuffer
}

void PackAttributeMerger::ReleaseMemBuffers()
{
    mPatchBuffer.Release();
    mMergeBuffer.Release();
}
}} // namespace indexlib::index

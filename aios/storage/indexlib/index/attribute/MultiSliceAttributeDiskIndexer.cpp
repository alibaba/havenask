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
#include "indexlib/index/attribute/MultiSliceAttributeDiskIndexer.h"

#include "indexlib/file_system/Directory.h"
#include "indexlib/index/attribute/SliceInfo.h"

namespace indexlibv2::index {
AUTIL_LOG_SETUP(indexlib.index, MultiSliceAttributeDiskIndexer);

Status MultiSliceAttributeDiskIndexer::Open(const std::shared_ptr<config::IIndexConfig>& indexConfig,
                                            const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    if (!_creator) {
        AUTIL_LOG(ERROR, "[%s] no attribute creator, open failed", indexConfig->GetIndexName().c_str());
        return Status::InternalError();
    }
    int64_t docCount = _indexerParam.docCount;
    if (docCount == 0) {
        AUTIL_LOG(INFO, "doc count is zero in index [%s] for segment [%d], just do nothing",
                  indexConfig->GetIndexName().c_str(), _indexerParam.segmentId);
        return Status::OK();
    }
    auto attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
    assert(attrConfig != nullptr);
    auto attrPath = GetAttributePath(attrConfig);
    auto [status, isExist] = indexDirectory->IsExist(attrPath).StatusWith();
    RETURN_IF_STATUS_ERROR(status, "IsExist attribute dir [%s] failed", attrPath.c_str());
    if (!isExist) {
        return AddSliceIndexer(indexConfig, indexDirectory, 0, docCount);
    }
    auto [status1, fieldDirectory] = indexDirectory->GetDirectory(attrPath).StatusWith();
    RETURN_IF_STATUS_ERROR(status1, "get field dir [%s] failed", attrPath.c_str());
    bool isDataInfoExist = false;
    status = _dataInfo.Load(fieldDirectory, isDataInfoExist);
    RETURN_IF_STATUS_ERROR(status, "load data info failed");
    if (isDataInfoExist && _dataInfo.sliceCount > 1) {
        auto sliceConfigs = attrConfig->CreateSliceAttributeConfigs(_dataInfo.sliceCount);

        for (int64_t sliceIdx = 0; sliceIdx < _dataInfo.sliceCount; sliceIdx++) {
            SliceInfo sliceInfo(_dataInfo.sliceCount, sliceIdx);
            docid_t beginDocId = INVALID_DOCID;
            docid_t endDocId = INVALID_DOCID;
            sliceInfo.GetDocRange(docCount, beginDocId, endDocId);
            if (beginDocId > endDocId) {
                // no data, continue
                continue;
            }
            status = AddSliceIndexer(sliceConfigs[sliceIdx], indexDirectory, beginDocId, endDocId - beginDocId + 1);
            RETURN_IF_STATUS_ERROR(status, "open slice indexer fail");
        }
        return Status::OK();
    }
    return AddSliceIndexer(indexConfig, indexDirectory, 0, docCount);
}

Status MultiSliceAttributeDiskIndexer::AddSliceIndexer(
    const std::shared_ptr<config::IIndexConfig>& indexConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory, docid_t baseDocId, int64_t docCount)
{
    std::shared_ptr<IDiskIndexer> indexer = _creator->Create(_attributeMetrics, _indexerParam);
    auto attrIndexer = std::dynamic_pointer_cast<AttributeDiskIndexer>(indexer);
    if (!attrIndexer) {
        AUTIL_LOG(ERROR, "[%s] get attribute indexer failed", indexConfig->GetIndexName().c_str());
        return Status::InternalError();
    }
    auto status = attrIndexer->Open(indexConfig, indexDirectory);
    RETURN_IF_STATUS_ERROR(status, "open attr disk indexer failed");
    _sliceAttributes.push_back(attrIndexer);
    _sliceBaseDocIds.push_back(baseDocId);
    _sliceDocCounts.push_back(docCount);
    auto dataInfo = attrIndexer->GetAttributeDataInfo();
    _dataInfo.uniqItemCount += dataInfo.uniqItemCount;
    _dataInfo.maxItemLen = std::max(_dataInfo.maxItemLen, dataInfo.maxItemLen);
    return Status::OK();
}

std::shared_ptr<AttributeDiskIndexer::ReadContextBase>
MultiSliceAttributeDiskIndexer::CreateReadContextPtr(autil::mem_pool::Pool* pool) const
{
    auto multiSliceReadContext = std::make_shared<MultiSliceReadContext>();
    for (auto sliceAttribute : _sliceAttributes) {
        multiSliceReadContext->sliceReadContexts.push_back(sliceAttribute->CreateReadContextPtr(pool));
    }
    return multiSliceReadContext;
}

std::shared_ptr<AttributeDiskIndexer::ReadContextBase> MultiSliceAttributeDiskIndexer::GetSliceReadContextPtr(
    const std::shared_ptr<AttributeDiskIndexer::ReadContextBase>& ctx, size_t sliceIdx) const
{
    auto multiSliceReadContext = std::dynamic_pointer_cast<MultiSliceReadContext>(ctx);
    assert(multiSliceReadContext);
    assert(sliceIdx < multiSliceReadContext->sliceReadContexts.size());
    return multiSliceReadContext->sliceReadContexts[sliceIdx];
}

bool MultiSliceAttributeDiskIndexer::UpdateField(docid_t docId, const autil::StringView& value, bool isNull,
                                                 const uint64_t* hashKey)
{
    int64_t sliceIdx = GetSliceIdxByDocId(docId);
    if (sliceIdx == -1) {
        return false;
    }
    return _sliceAttributes[sliceIdx]->UpdateField(docId - _sliceBaseDocIds[sliceIdx], value, isNull, hashKey);
}

bool MultiSliceAttributeDiskIndexer::Read(docid_t docId, const std::shared_ptr<ReadContextBase>& ctx, uint8_t* buf,
                                          uint32_t bufLen, uint32_t& dataLen, bool& isNull)
{
    int64_t sliceIdx = GetSliceIdxByDocId(docId);
    if (sliceIdx == -1) {
        return false;
    }
    return _sliceAttributes[sliceIdx]->Read(docId - _sliceBaseDocIds[sliceIdx], GetSliceReadContextPtr(ctx, sliceIdx),
                                            buf, bufLen, dataLen, isNull);
}

Status MultiSliceAttributeDiskIndexer::SetPatchReader(const std::shared_ptr<AttributePatchReader>& patchReader,
                                                      docid_t patchBaseDocId)
{
    size_t i = 0;
    for (auto sliceReader : _sliceAttributes) {
        auto status = sliceReader->SetPatchReader(patchReader, _sliceBaseDocIds[i]);
        i++;
        RETURN_IF_STATUS_ERROR(status, "set patch reader failed");
        auto dataInfo = sliceReader->GetAttributeDataInfo();
        _dataInfo.maxItemLen = std::max(_dataInfo.maxItemLen, dataInfo.maxItemLen);
    }
    return Status::OK();
}

std::pair<Status, uint64_t> MultiSliceAttributeDiskIndexer::GetOffset(docid_t docId,
                                                                      const std::shared_ptr<ReadContextBase>& ctx) const
{
    int64_t sliceIdx = GetSliceIdxByDocId(docId);
    if (sliceIdx == -1) {
        return std::make_pair(Status::Corruption(), 0);
    }
    return _sliceAttributes[sliceIdx]->GetOffset(docId - _sliceBaseDocIds[sliceIdx],
                                                 GetSliceReadContextPtr(ctx, sliceIdx));
}

bool MultiSliceAttributeDiskIndexer::Read(docid_t docId, std::string* value, autil::mem_pool::Pool* pool)
{
    int64_t sliceIdx = GetSliceIdxByDocId(docId);
    if (sliceIdx == -1) {
        return false;
    }
    return _sliceAttributes[sliceIdx]->Read(docId - _sliceBaseDocIds[sliceIdx], value, pool);
}

uint32_t MultiSliceAttributeDiskIndexer::TEST_GetDataLength(docid_t docId, autil::mem_pool::Pool* pool) const
{
    int64_t sliceIdx = GetSliceIdxByDocId(docId);
    if (sliceIdx == -1) {
        return 0;
    }
    return _sliceAttributes[sliceIdx]->TEST_GetDataLength(docId - _sliceBaseDocIds[sliceIdx], pool);
}

int64_t MultiSliceAttributeDiskIndexer::GetSliceIdxByDocId(docid_t docId) const
{
    for (size_t i = 0; i < _sliceAttributes.size(); i++) {
        if (_sliceBaseDocIds[i] <= docId && _sliceDocCounts[i] + _sliceBaseDocIds[i] > docId) {
            return i;
        }
    }
    AUTIL_LOG(ERROR, "docId [%d] not in slice ranges", docId);
    return -1;
}

size_t MultiSliceAttributeDiskIndexer::EstimateMemUsed(
    const std::shared_ptr<config::IIndexConfig>& indexConfig,
    const std::shared_ptr<indexlib::file_system::IDirectory>& indexDirectory)
{
    size_t estimateMemUse = 0;
    if (!indexDirectory) {
        return 0;
    }
    auto attrConfig = std::dynamic_pointer_cast<config::AttributeConfig>(indexConfig);
    assert(attrConfig != nullptr);
    auto [status, attrDirectory] = indexDirectory->GetDirectory(attrConfig->GetAttrName()).StatusWith();
    if (!status.IsOK()) {
        return 0;
    }
    bool isDataInfoExist = false;
    status = _dataInfo.Load(attrDirectory, isDataInfoExist);
    if (!status.IsOK()) {
        return 0;
    }
    int64_t docCount = _indexerParam.docCount;
    std::shared_ptr<IDiskIndexer> indexer = _creator->Create(_attributeMetrics, _indexerParam);
    if (isDataInfoExist && _dataInfo.sliceCount > 1) {
        auto sliceConfigs = attrConfig->CreateSliceAttributeConfigs(_dataInfo.sliceCount);
        for (int64_t sliceIdx = 0; sliceIdx < _dataInfo.sliceCount; sliceIdx++) {
            SliceInfo sliceInfo(_dataInfo.sliceCount, sliceIdx);
            docid_t beginDocId = INVALID_DOCID;
            docid_t endDocId = INVALID_DOCID;
            sliceInfo.GetDocRange(docCount, beginDocId, endDocId);
            if (beginDocId > endDocId) {
                // no data, continue
                continue;
            }
            estimateMemUse += indexer->EstimateMemUsed(sliceConfigs[sliceIdx], indexDirectory);
        }
        return estimateMemUse;
    }
    return indexer->EstimateMemUsed(indexConfig, indexDirectory);
}

size_t MultiSliceAttributeDiskIndexer::EvaluateCurrentMemUsed()
{
    size_t memUse = 0;
    for (auto slice : _sliceAttributes) {
        memUse += slice->EvaluateCurrentMemUsed();
    }
    return memUse;
}

} // namespace indexlibv2::index

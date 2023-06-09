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
#include "indexlib/index/inverted_index/truncate/TruncateAttributeReader.h"

#include "autil/mem_pool/Pool.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/index/DocMapper.h"
#include "indexlib/index/attribute/AttributeDiskIndexer.h"
#include "indexlib/index/attribute/config/AttributeConfig.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(indexlib.index, TruncateAttributeReader);

void TruncateAttributeReader::AddAttributeDiskIndexer(
    segmentid_t segId, const std::shared_ptr<indexlibv2::index::AttributeDiskIndexer>& diskIndexer)
{
    _diskIndexers[segId] = diskIndexer;
}

std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>
TruncateAttributeReader::CreateReadContextPtr(autil::mem_pool::Pool* pool) const
{
    bool success = false;
    auto ctx = std::make_shared<ReadContext>();
    for (const auto& [segId, diskIndexer] : _diskIndexers) {
        if (diskIndexer != nullptr) {
            success = true;
            ctx->segCtx[segId] = diskIndexer->CreateReadContextPtr(pool);
        }
    }
    if (!success) {
        AUTIL_LOG(ERROR, "no valid disk indexer in truncate attribute reader.");
        return nullptr;
    }
    return ctx;
}

bool TruncateAttributeReader::Read(docid_t docId,
                                   const std::shared_ptr<indexlibv2::index::AttributeDiskIndexer::ReadContextBase>& ctx,
                                   uint8_t* buf, uint32_t bufLen, uint32_t& dataLen, bool& isNull)
{
    auto typedCtx = std::dynamic_pointer_cast<ReadContext>(ctx);
    auto [srcSegmentId, srcLocalDocId] = _docMapper->ReverseMap(docId);

    auto diskIndexer = _diskIndexers[srcSegmentId];
    assert(diskIndexer != nullptr);
    return diskIndexer->Read(srcLocalDocId, typedCtx->segCtx[srcSegmentId], buf, bufLen, dataLen, isNull);
}

} // namespace indexlib::index

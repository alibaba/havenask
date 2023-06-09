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
#include "indexlib/index/merger_util/truncate/truncate_attribute_reader.h"

using namespace std;

using namespace indexlib::config;
namespace indexlib::index::legacy {
IE_LOG_SETUP(index, TruncateAttributeReader);

TruncateAttributeReader::TruncateAttributeReader() {}

TruncateAttributeReader::~TruncateAttributeReader() {}

void TruncateAttributeReader::Init(const ReclaimMapPtr& relaimMap, const AttributeConfigPtr& attrConfig)
{
    mReclaimMap = relaimMap;
    mAttrConfig = attrConfig;
}

void TruncateAttributeReader::AddAttributeSegmentReader(segmentid_t segId,
                                                        const AttributeSegmentReaderPtr& segmentReader)
{
    if (segId >= (segmentid_t)mReaders.size()) {
        mReaders.resize(segId + 1, AttributeSegmentReaderPtr());
    }
    assert(mReaders[segId] == NULL);
    mReaders[segId] = segmentReader;
}

AttributeSegmentReader::ReadContextBasePtr
TruncateAttributeReader::CreateReadContextPtr(autil::mem_pool::Pool* pool) const
{
    ReadContext* ctx = new ReadContext;
    ctx->segCtx.resize(mReaders.size());
    for (size_t i = 0; i < mReaders.size(); ++i) {
        if (mReaders[i]) {
            ctx->segCtx[i] = mReaders[i]->CreateReadContextPtr(pool);
        }
    }
    return ReadContextBasePtr(ctx);
}

bool TruncateAttributeReader::Read(docid_t docId, const ReadContextBasePtr& ctx, uint8_t* buf, uint32_t bufLen,
                                   bool& isNull)
{
    segmentid_t segId = INVALID_SEGMENTID;
    ReadContext* typedCtx = (ReadContext*)ctx.get();
    docid_t localDocId = mReclaimMap->GetOldDocIdAndSegId(docId, segId);
    assert(segId != INVALID_SEGMENTID && segId < (segmentid_t)mReaders.size());
    auto segReader = mReaders[segId];
    assert(segReader);
    return segReader->Read(localDocId, typedCtx->segCtx[segId], buf, bufLen, isNull);
}
} // namespace indexlib::index::legacy

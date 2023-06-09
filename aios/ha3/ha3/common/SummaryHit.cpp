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
#include "ha3/common/SummaryHit.h"

#include <cstddef>
#include <vector>

#include "autil/ConstString.h"
#include "autil/DataBuffer.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "autil/mem_pool/Pool.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "indexlib/indexlib.h"

#include "ha3/common/Tracer.h"
#include "ha3/config/HitSummarySchema.h"
#include "autil/Log.h"

using namespace std;
using namespace autil::mem_pool;
using namespace indexlib::document;

using namespace isearch::config;

namespace isearch {
namespace common {
AUTIL_LOG_SETUP(ha3, SummaryHit);

SummaryHit::SummaryHit(HitSummarySchema *hitSummarySchema,
                       Pool *pool, Tracer* tracer)
    : _hitSummarySchema(hitSummarySchema)
    , _summaryDoc(pool, hitSummarySchema ? hitSummarySchema->getFieldCount() : 0)
    , _tracer(tracer)
{
}

SummaryHit::~SummaryHit() {
}

void SummaryHit::setSummaryValue(const string &fieldName, const string &fieldValue) {
    setSummaryValue(fieldName, fieldValue.c_str(), fieldValue.size(), true);
}

void SummaryHit::setSummaryValue(const string &fieldName, const char* fieldValue,
                                 size_t size, bool needCopy)
{
    const SummaryFieldInfo *summaryInfo = _hitSummarySchema->getSummaryFieldInfo(fieldName);
    summaryfieldid_t summaryFieldId;
    if (summaryInfo) {
        summaryFieldId = summaryInfo->summaryFieldId;
    } else {
        summaryFieldId = _hitSummarySchema->declareSummaryField(fieldName);
    }
    _summaryDoc.SetFieldValue(summaryFieldId, fieldValue, size, needCopy);
}

void SummaryHit::serialize(autil::DataBuffer &dataBuffer) const {
    dataBuffer.write(_signature);
    const std::vector<autil::StringView*>& fields = _summaryDoc.GetFields();
    uint32_t size = static_cast<uint32_t>(fields.size());
    dataBuffer.write(size);
    for (uint32_t i = 0; i < size; i++) {
        autil::StringView *p = fields[i];
        if (!p) {
            dataBuffer.write(false);
            continue;
        }
        dataBuffer.write(true);
        dataBuffer.write(static_cast<uint32_t>(p->size()));
        dataBuffer.writeBytes(p->data(), p->size());
    }
}

void SummaryHit::deserialize(autil::DataBuffer &dataBuffer) {
    _hitSummarySchema = NULL;

    dataBuffer.read(_signature);
    uint32_t size = 0;
    dataBuffer.read(size);
    _summaryDoc.UpdateFieldCount(size);
    for (uint32_t i = 0; i < size; ++i) {
        bool hasValue = false;
        dataBuffer.read(hasValue);
        if (!hasValue) {
            continue;
        } else {
            uint32_t strLen = 0;
            dataBuffer.read(strLen);
            const char *data = (const char*)dataBuffer.readNoCopy(strLen);
            _summaryDoc.SetFieldValue(i, data, strLen);
        }
    }
}

void SummaryHit::summarySchemaToSignature() {
    // _hitSummarySchema is NULL in proxy.
    if (_hitSummarySchema) {
        _signature = _hitSummarySchema->getSignature();
        _hitSummarySchema = NULL;
    }
}

} // namespace common
} // namespace isearch

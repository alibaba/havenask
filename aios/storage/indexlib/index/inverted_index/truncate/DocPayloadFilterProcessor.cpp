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
#include "indexlib/index/inverted_index/truncate/DocPayloadFilterProcessor.h"

#include "autil/StringUtil.h"

namespace indexlib::index {
AUTIL_LOG_SETUP(index, DocPayloadFilterProcessor);

DocPayloadFilterProcessor::DocPayloadFilterProcessor(const indexlibv2::config::DiversityConstrain& constrain,
                                                     const std::shared_ptr<IEvaluator>& evaluator,
                                                     const std::shared_ptr<DocInfoAllocator>& allocator)
{
    _maxValue = constrain.GetFilterMaxValue();
    _minValue = constrain.GetFilterMinValue();
    _mask = constrain.GetFilterMask();
    _evaluator = evaluator;
    _sortFieldRef = nullptr;
    _docInfo = nullptr;
    if (allocator) {
        _sortFieldRef = allocator->GetReference(DOC_PAYLOAD_FIELD_NAME);
        _docInfo = allocator->Allocate();
    }
}

bool DocPayloadFilterProcessor::BeginFilter(const DictKeyInfo& key, const std::shared_ptr<PostingIterator>& postingIt)
{
    _postingIt = postingIt;
    if (_metaReader && !_metaReader->Lookup(key, _minValue, _maxValue)) {
        return false;
    }

    return true;
}

bool DocPayloadFilterProcessor::IsFiltered(docid_t docId)
{
    assert(_postingIt);
    if (_evaluator) {
        _docInfo->SetDocId(docId);
        _evaluator->Evaluate(docId, _postingIt, _docInfo);
        std::string valueStr = _sortFieldRef->GetStringValue(_docInfo);
        double value;
        if (!autil::StringUtil::fromString(valueStr, value)) {
            AUTIL_LOG(ERROR, "invalid value[%s], will be filtered.", valueStr.c_str());
            return true;
        }
        int64_t valueInt64 = (int64_t)value;
        valueInt64 &= _mask;
        return !(_minValue <= valueInt64 && valueInt64 <= _maxValue);
    } else {
        docpayload_t value = _postingIt->GetDocPayload();
        value &= (docpayload_t)_mask;
        return !(_minValue <= (int64_t)value && (int64_t)value <= _maxValue);
    }
}

} // namespace indexlib::index

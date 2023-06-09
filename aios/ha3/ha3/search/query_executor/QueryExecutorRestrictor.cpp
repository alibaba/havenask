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
#include "ha3/search/QueryExecutorRestrictor.h"

#include <algorithm>
#include <cstddef>

#include "autil/mem_pool/PoolVector.h"

#include "ha3/common/TimeoutTerminator.h"
#include "ha3/isearch.h"
#include "ha3/search/LayerMetas.h"
#include "autil/Log.h"

using namespace std;

namespace isearch {
namespace search {
AUTIL_LOG_SETUP(ha3, QueryExecutorRestrictor);

LayerMetaWrapper::LayerMetaWrapper(const LayerMeta *layerMeta)
 : _layerMeta(layerMeta)
{
    reset();
}

docid_t LayerMetaWrapper::seek(docid_t curDocId) {
    while (_offset < _layerMeta->size() && 
           (*_layerMeta)[_offset].end < curDocId)
    {
        ++_offset;
    }
    if (_offset < _layerMeta->size()) {
        return max(curDocId, (*_layerMeta)[_offset].begin);
    } else {
        return END_DOCID;
    }
}

void LayerMetaWrapper::reset() {
    _offset = 0;
}

LayerMetaWrapper::~LayerMetaWrapper () {
}

QueryExecutorRestrictor::QueryExecutorRestrictor() 
    : _timeoutTerminator(NULL)
    , _layerMetaWrapper(NULL)
{ 
}

QueryExecutorRestrictor::~QueryExecutorRestrictor() { 
    DELETE_AND_SET_NULL(_layerMetaWrapper);
}

docid_t QueryExecutorRestrictor::meetRestrict(docid_t curDocId) {
    if (_timeoutTerminator && _timeoutTerminator->checkRestrictTimeout()) {
        return END_DOCID;
    }
    if (_layerMetaWrapper) {
        return _layerMetaWrapper->seek(curDocId);
    }
    return curDocId;
}

void QueryExecutorRestrictor::reset() {
    if (_layerMetaWrapper) {
        _layerMetaWrapper->reset();
    }
}
} // namespace search
} // namespace isearch


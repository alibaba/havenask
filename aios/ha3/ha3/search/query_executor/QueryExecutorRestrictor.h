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

#include <stddef.h>
#include <stdint.h>

#include "autil/CommonMacros.h"
#include "indexlib/indexlib.h"

#include "ha3/common/TimeoutTerminator.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace search {
class LayerMeta;
}  // namespace search
}  // namespace isearch

namespace isearch {
namespace search {

class LayerMetaWrapper {
public:
    LayerMetaWrapper(const LayerMeta *layerMeta);
    ~LayerMetaWrapper();
private:
    LayerMetaWrapper(const LayerMetaWrapper&);
    LayerMetaWrapper& operator= (const LayerMetaWrapper &);
public:
    docid_t seek(docid_t docId);
    void reset();
private:
    const LayerMeta *_layerMeta;
    uint32_t _offset;
};

class QueryExecutorRestrictor
{
public:
    QueryExecutorRestrictor();
    ~QueryExecutorRestrictor();
private:
    QueryExecutorRestrictor(const QueryExecutorRestrictor &);
    QueryExecutorRestrictor& operator=(const QueryExecutorRestrictor &);
public:
    void setTimeoutTerminator(common::TimeoutTerminator *timeoutTerminator) {
        _timeoutTerminator = timeoutTerminator;
    }
    void setLayerMeta(const LayerMeta *layerMeta) {
        DELETE_AND_SET_NULL(_layerMetaWrapper);
        if (layerMeta != NULL) {
            _layerMetaWrapper = new LayerMetaWrapper(layerMeta);
        }
    }
    
    docid_t meetRestrict(docid_t curDocId);
    void reset();
private:
    common::TimeoutTerminator *_timeoutTerminator;
    LayerMetaWrapper *_layerMetaWrapper;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryExecutorRestrictor> QueryExecutorRestrictorPtr;

} // namespace search
} // namespace isearch


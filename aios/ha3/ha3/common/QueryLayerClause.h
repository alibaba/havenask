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
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"

namespace autil {
class DataBuffer;
}  // namespace autil
namespace isearch {
namespace common {
class LayerDescription;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace common {

class QueryLayerClause : public ClauseBase
{
public:
    QueryLayerClause();
    ~QueryLayerClause();
private:
    QueryLayerClause(const QueryLayerClause &);
    QueryLayerClause& operator=(const QueryLayerClause &);
public:
    void serialize(autil::DataBuffer &dataBuffer) const override;
    void deserialize(autil::DataBuffer &dataBuffer) override;
    std::string toString() const override;
public:
    size_t getLayerCount() const {
        return _layerDescriptions.size();
    }
    LayerDescription *getLayerDescription(size_t pos) const;
    void addLayerDescription(LayerDescription *layerDesc);
private:
    std::vector<LayerDescription*> _layerDescriptions;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<QueryLayerClause> QueryLayerClausePtr;

} // namespace common
} // namespace isearch


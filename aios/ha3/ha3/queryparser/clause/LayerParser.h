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

#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class LayerDescription;
class QueryLayerClause;
}  // namespace common
}  // namespace isearch

namespace isearch {
namespace queryparser {

class LayerParser
{
public:
    LayerParser();
    ~LayerParser();
private:
    LayerParser(const LayerParser &);
    LayerParser& operator=(const LayerParser &);
public:
    common::QueryLayerClause *createLayerClause();
    common::LayerDescription *createLayerDescription();
    void setQuota(common::LayerDescription *layerDesc, const std::string &quota);
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LayerParser> LayerParserPtr;

} // namespace queryparser
} // namespace isearch


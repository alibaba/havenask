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
#include "ha3/search/LayerMetas.h"
#include "tensorflow/core/framework/variant_tensor_data.h"

namespace isearch {
namespace turing {

class LayerMetasVariant
{
public:
    LayerMetasVariant() {}
    LayerMetasVariant(search::LayerMetasPtr layerMetas) :_layerMetas(layerMetas) {}
    ~LayerMetasVariant() {}

public:
    void Encode(tensorflow::VariantTensorData* data) const {}
    bool Decode(const tensorflow::VariantTensorData& data) {
        return false;
    }
    std::string TypeName() const {
        return "Ha3LayerMetas";
    }
public:
    search::LayerMetasPtr getLayerMetas() const {
        return _layerMetas;
    }

private:
    search::LayerMetasPtr _layerMetas;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<LayerMetasVariant> LayerMetasVariantPtr;

} // namespace turing
} // namespace isearch


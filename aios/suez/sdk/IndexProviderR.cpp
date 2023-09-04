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
#include "suez/sdk/IndexProviderR.h"

namespace suez {

const std::string IndexProviderR::RESOURCE_ID = "suez.index_provider_r";

IndexProviderR::IndexProviderR() : navi::RootResource(RESOURCE_ID) {}

IndexProviderR::IndexProviderR(const suez::IndexProvider &indexProvider)
    : navi::RootResource(RESOURCE_ID), _indexProvider(indexProvider) {}

IndexProviderR::~IndexProviderR() {}

const suez::IndexProvider &IndexProviderR::getIndexProvider() const { return _indexProvider; }

REGISTER_RESOURCE(IndexProviderR);

} // namespace suez

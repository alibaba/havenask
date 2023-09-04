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

#include "navi/engine/Resource.h"
#include "suez/sdk/IndexProvider.h"

namespace suez {

class IndexProviderR : public navi::RootResource {
public:
    IndexProviderR();
    IndexProviderR(const suez::IndexProvider &indexProvider);
    ~IndexProviderR();
    IndexProviderR(const IndexProviderR &) = delete;
    IndexProviderR &operator=(const IndexProviderR &) = delete;

public:
    const suez::IndexProvider &getIndexProvider() const;

public:
    static const std::string RESOURCE_ID;

private:
    suez::IndexProvider _indexProvider;
};

NAVI_TYPEDEF_PTR(IndexProviderR);

} // namespace suez

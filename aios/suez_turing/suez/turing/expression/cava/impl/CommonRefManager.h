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
#include <string>

#include "suez/turing/expression/cava/impl/FieldRef.h"
#include "suez/turing/expression/cava/impl/IRefManager.h"

class CavaCtx;
namespace matchdoc {
class ReferenceBase;
} // namespace matchdoc
namespace suez {
namespace turing {
class ProviderBase;
} // namespace turing
} // namespace suez

namespace ha3 {

class CommonRefManager : public ha3::IRefManager {
public:
    CommonRefManager(suez::turing::ProviderBase *provider, bool enableRequire = true, bool enableDeclare = true)
        : _provider(provider), _enableRequire(enableRequire), _enableDeclare(enableDeclare) {}
    ~CommonRefManager() {}

private:
    CommonRefManager(const CommonRefManager &);
    CommonRefManager &operator=(const CommonRefManager &);

public:
    matchdoc::ReferenceBase *require(CavaCtx *ctx, const std::string &name) override;
    matchdoc::ReferenceBase *
    declare(CavaCtx *ctx, const std::string &name, ha3::FieldRefType type, bool serialize) override;
    matchdoc::ReferenceBase *find(CavaCtx *ctx, const std::string &name) override;

private:
    suez::turing::ProviderBase *_provider;
    bool _enableRequire;
    bool _enableDeclare;
};

} // namespace ha3

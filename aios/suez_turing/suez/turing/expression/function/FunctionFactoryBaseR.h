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

namespace suez {
namespace turing {

class SyntaxExpressionFactory;

class FunctionFactoryBaseR : public navi::Resource {
public:
    FunctionFactoryBaseR() {}
    ~FunctionFactoryBaseR() {}
    FunctionFactoryBaseR(const FunctionFactoryBaseR &) = delete;
    FunctionFactoryBaseR &operator=(const FunctionFactoryBaseR &) = delete;

public:
    suez::turing::SyntaxExpressionFactory *getFactory() const {
        return _factory.get();
    }
protected:
    bool config(navi::ResourceConfigContext &ctx) override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;
protected:
    virtual std::shared_ptr<suez::turing::SyntaxExpressionFactory> createFactory() = 0;
protected:
    std::string _configPath;
    std::shared_ptr<suez::turing::SyntaxExpressionFactory> _factory;
};

NAVI_TYPEDEF_PTR(FunctionFactoryBaseR);

} // namespace turing
} // namespace suez

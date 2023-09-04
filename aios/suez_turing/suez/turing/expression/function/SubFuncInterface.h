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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/SubDocAccessor.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/function/FunctionMap.h"
#include "suez/turing/expression/function/ProcessorWrapper.h"
#include "suez/turing/expression/provider/FunctionProvider.h"

namespace matchdoc {
class ReferenceBase;
} // namespace matchdoc

namespace suez {
namespace turing {

template <typename ResultType, typename Processor>
class SubFuncInterface : public FunctionInterfaceTyped<ResultType> {
public:
    SubFuncInterface(const Processor &processor) : _processor(processor) {}
    ~SubFuncInterface() {}

public:
    bool beginRequest(FunctionProvider *provider) override {
        auto pool = provider->getPool();
        assert(pool);
        _accessor = provider->getSubDocAccessor();
        if (!_accessor) {
            AUTIL_LOG(ERROR, "SubSlabAccessor is NULL, sub doc disabled.");
            return false;
        }
        _processor.setPool(pool);
        return true;
    }

    ResultType evaluate(matchdoc::MatchDoc matchDoc) override {
        assert(matchdoc::INVALID_MATCHDOC != matchDoc);
        assert(_accessor);
        _processor.begin();
        _accessor->foreach (matchDoc, _processor);
        return _processor.getResult();
    }

private:
    Processor _processor;
    matchdoc::SubDocAccessor *_accessor;

private:
    AUTIL_LOG_DECLARE();
};

class SubJoinVarFuncInterface : public FunctionInterfaceTyped<autil::MultiChar> {
public:
    SubJoinVarFuncInterface(const std::string &varName, const std::string &split)
        : _varName(varName), _split(split), _processor(NULL, _split) {}
    ~SubJoinVarFuncInterface() {}

public:
    bool beginRequest(FunctionProvider *provider) override {
        auto pool = provider->getPool();
        assert(pool);
        _accessor = provider->getSubDocAccessor();
        if (!_accessor) {
            AUTIL_LOG(ERROR, "SubSlabAccessor is NULL, sub doc disabled.");
            return false;
        }
        _varRef = provider->findVariableReferenceWithoutType(_varName);
        if (!_varRef) {
            AUTIL_LOG(ERROR, "[%s] variable reference is not exist!", _varName.c_str());
            return false;
        }
        _processor = JoinVarProcessor(_varRef, _split);
        _processor.setPool(pool);
        return true;
    }

    autil::MultiChar evaluate(matchdoc::MatchDoc matchDoc) override {
        assert(matchdoc::INVALID_MATCHDOC != matchDoc);
        assert(_accessor);
        _processor.begin();
        _accessor->foreach (matchDoc, _processor);
        return _processor.getResult();
    }

private:
    std::string _varName;
    std::string _split;
    JoinVarProcessor _processor;
    matchdoc::ReferenceBase *_varRef;
    matchdoc::SubDocAccessor *_accessor;

private:
    AUTIL_LOG_DECLARE();
};

template <typename ResultType, typename Processor>
alog::Logger *SubFuncInterface<ResultType, Processor>::_logger =
    alog::Logger::getLogger("suez.turing.expression.SubFuncInterface");

DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(SubFirstFuncInterface, "sub_first", 1, FUNC_ACTION_SCOPE_MAIN_DOC);
DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(SubMinFuncInterface, "sub_min", 1, FUNC_ACTION_SCOPE_MAIN_DOC);
DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(SubMaxFuncInterface, "sub_max", 1, FUNC_ACTION_SCOPE_MAIN_DOC);
DECLARE_UNKNOWN_TYPE_FUNCTION_CREATOR(SubSumFuncInterface, "sub_sum", 1, FUNC_ACTION_SCOPE_MAIN_DOC);
DECLARE_TEMPLATE_FUNCTION_CREATOR(SubCountFuncInterface, uint32_t, "sub_count", 0, FUNC_ACTION_SCOPE_MAIN_DOC);
DECLARE_TEMPLATE_FUNCTION_CREATOR(
    SubJoinFuncInterface, autil::MultiChar, "sub_join", FUNCTION_UNLIMITED_ARGUMENT_COUNT, FUNC_ACTION_SCOPE_MAIN_DOC);

class SubFirstFuncInterfaceCreatorImpl : public SubFirstFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;

private:
    AUTIL_LOG_DECLARE();
};

class SubMinFuncInterfaceCreatorImpl : public SubMinFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

class SubMaxFuncInterfaceCreatorImpl : public SubMaxFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

class SubSumFuncInterfaceCreatorImpl : public SubSumFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

class SubCountFuncInterfaceCreatorImpl : public SubCountFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;
};

class SubJoinFuncInterfaceCreatorImpl : public SubJoinFuncInterfaceCreator {
public:
    FunctionInterface *createFunction(const FunctionSubExprVec &funcSubExprVec) override;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace suez

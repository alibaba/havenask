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
#ifndef ISEARCH_EXPRESSION_SUBFUNCINTERFACE_H
#define ISEARCH_EXPRESSION_SUBFUNCINTERFACE_H

#include "expression/common.h"
#include "expression/function/FunctionInterface.h"
#include "expression/function/SubJoinProcessor.h"

namespace expression {

template<typename T, typename Processor>
class SubFuncInterface : public FunctionInterfaceTyped<T>
{
private:
    typedef std::shared_ptr<Processor> ProcessorPtr;

public:
    SubFuncInterface(const ProcessorPtr& processor)
        : _processor(processor)
        , _accessor(NULL)
    { }

    ~SubFuncInterface() { }

private:
    SubFuncInterface(const SubFuncInterface& other);
public:
    /* override */ bool beginRequest(FunctionProvider *provider) {
        _accessor = provider->getSubDocAccessor();
        if (!_accessor) {
            AUTIL_LOG(ERROR, "SubDocAccessor is NULL, sub doc disabled.");
            return false;
        }
        return true;
    }

    /* override */ T evaluate(const matchdoc::MatchDoc &matchDoc) {
        assert(_accessor);
        _processor->begin();
        _accessor->foreach(matchDoc, *_processor);
        return _processor->getResult();
    }

protected:
    ProcessorPtr _processor;
    matchdoc::SubDocAccessor *_accessor;
protected:
    AUTIL_LOG_DECLARE();
};

template<typename T, typename Processor>
alog::Logger *SubFuncInterface<T, Processor>::_logger = alog::Logger::getLogger("expression.SubFuncInterface");

//////////////////////////////////////////////////////////////////////////
class SubJoinVarFuncInterface : 
        public SubFuncInterface<autil::MultiChar, SubJoinVarProcessor>
{
public:
    typedef SubJoinVarProcessor Processor;
    typedef std::shared_ptr<Processor> ProcessorPtr;

public:
    SubJoinVarFuncInterface(
            const std::string& varName,
            const std::string& split)
        : SubFuncInterface(ProcessorPtr())
        , _varName(varName)
        , _split(split)
    {}

    /* override */ bool beginRequest(FunctionProvider *provider) {
        _accessor = provider->getSubDocAccessor();
        if (!_accessor) {
            AUTIL_LOG(ERROR, "SubDocAccessor is NULL, sub doc disabled.");
            return false;
        }

        matchdoc::ReferenceBase* ref = 
            provider->findVariableReferenceWithoutType(_varName);
        if (!ref) {
            AUTIL_LOG(ERROR, "[%s] variable reference is not exist!",
                      _varName.c_str());
            return false;
        }
        _processor.reset(new Processor(ref, _split));
        return true;
    }

private:
    std::string _varName;
    std::string _split;
};

}

#endif //ISEARCH_EXPRESSION_SUBFUNCINTERFACE_H

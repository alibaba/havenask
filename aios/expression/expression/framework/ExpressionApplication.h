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
#ifndef ISEARCH_EXPRESSION_EXPRESSIONAPPLICATION_H
#define ISEARCH_EXPRESSION_EXPRESSIONAPPLICATION_H

#include "expression/common.h"
#include "expression/framework/AttributeExpression.h"
#include "expression/framework/AttributeExpressionCreator.h"
#include "expression/function/FunctionInterfaceFactory.h"
#include "autil/Lock.h"

namespace expression {
class FunctionInterfaceManager;

class ExpressionApplication
{
public:
    typedef std::deque<AttributeExpressionCreatorPtr> SessionCreatorVec;
    typedef std::tr1::unordered_map<pthread_t, SessionCreatorVec> CreatorPool;
public:
    ExpressionApplication(std::function<AtomicAttributeExpressionCreatorBase *(AttributeExpressionPool*, autil::mem_pool::Pool*, bool)> atomicCreator);
    ~ExpressionApplication();
public:
    // Note: SimpleValue is not allowed to copy
    // so pass pointer and make sure the life cycle of va outside
    bool registerVirtualAttributes(const SimpleValue *va);

    bool registerFunctions(const FunctionConfig &funcConfig,
                           const resource_reader::ResourceReaderPtr &resourceReaderPtr,
                           FunctionInterfaceFactory *functionFactory);

    bool registerFunctions(const FunctionConfig &funcConfig,
                           const resource_reader::ResourceReaderPtr &resourceReaderPtr,
                           const FunctionInterfaceFactoryPtr &functionFactoryPtr)
    {
        _functionFactoryPtrVec.push_back(functionFactoryPtr);
        return registerFunctions(funcConfig, resourceReaderPtr, functionFactoryPtr.get());
    }

public:
    AttributeExpressionCreator *create(
            bool useSub = true, autil::mem_pool::Pool* pool = NULL) const;
    
private:
    ExpressionApplication(const ExpressionApplication &);
    ExpressionApplication& operator=(const ExpressionApplication &);
private:
    //    AttributeInfoMapPtr _attrInfoMap;
    std::function<AtomicAttributeExpressionCreatorBase *(AttributeExpressionPool*, autil::mem_pool::Pool*, bool)> _atomicCreator;
    const SimpleValue *_virtualAttributes;
    FunctionInterfaceManager *_functionInterfaceManager;
    CreatorPool _creators;
    std::vector<FunctionInterfaceFactoryPtr> _functionFactoryPtrVec;
    autil::ReadWriteLock _lock;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ExpressionApplication> ExpressionApplicationPtr;
}

#endif //ISEARCH_EXPRESSION_EXPRESSIONAPPLICATION_H

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
#ifndef ISEARCH_EXPRESSION_SESSIONRESOURCEPROVIDER_H
#define ISEARCH_EXPRESSION_SESSIONRESOURCEPROVIDER_H

#include "expression/common.h"
#include "expression/common/SessionResource.h"

namespace expression {

class SessionResourceProvider
{
public:
    static const std::string VARIABLE_GROUP_NAME;
public:
    SessionResourceProvider(const SessionResource &resource);
    virtual ~SessionResourceProvider();
public:
    template<typename T>
    matchdoc::Reference<T>* declare(const std::string &name, SerializeLevel sl = SL_PROXY) {
        std::string newVariName = innerVariableName(name);
        if (_isSubScope) {
            return _sessionResource->allocator->declareSub<T>(
                    newVariName, VARIABLE_GROUP_NAME, sl);
        }
        return _sessionResource->allocator->declare<T>(newVariName,
                VARIABLE_GROUP_NAME, sl);
    }

    template<typename T>
    matchdoc::Reference<T>* declareSub(const std::string &name, SerializeLevel sl = SL_PROXY) {
        std::string newVariName = innerVariableName(name);
        return _sessionResource->allocator->declareSub<T>(newVariName,
                VARIABLE_GROUP_NAME, sl);
    }

    template<typename T> 
    matchdoc::Reference<T>* findVariableReference(const std::string &name) const
    {
        std::string newVariName = innerVariableName(name);
        return _sessionResource->allocator->findReference<T>(newVariName);
    }

    matchdoc::ReferenceBase* findVariableReferenceWithoutType(
            const std::string &name) const
    {
        std::string newVariName = innerVariableName(name);
        return _sessionResource->allocator->findReferenceWithoutType(newVariName);
    }

    matchdoc::SubDocAccessor *getSubDocAccessor() const {
        return _sessionResource->allocator->getSubDocAccessor();
    }
    
    const SessionResource *getSessionResource() const {
        return _sessionResource;
    }
    
    autil::mem_pool::Pool *getPool() const { return _sessionResource->sessionPool; }

    const SimpleDocument* getRequest() const { return _sessionResource->request; }

    FuncLocation getLocation() const { return _sessionResource->location; }

    SimpleDocument *getRequest() {
        return _sessionResource->request;
    }

    void addSortMeta(const std::string &name, matchdoc::ReferenceBase* ref, bool isAsc);
    const std::vector<SortMeta> &getSortMeta() const;

    static std::string getInnerVariableName(const std::string &name) {
        return innerVariableName(name);
    }

    bool isSubScope() const {
        return _isSubScope;
    }

    void setSubScope(bool isSubScope) {
        _isSubScope = isSubScope;
    }

    void unsetSubScope() {
        _isSubScope = false;
    }

private:
    static std::string innerVariableName(const std::string &name) {
        return PROVIDER_VAR_NAME_PREFIX + name;
    }

protected:
    const SessionResource *_sessionResource;
    bool _isSubScope;
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_SESSIONRESOURCEPROVIDER_H

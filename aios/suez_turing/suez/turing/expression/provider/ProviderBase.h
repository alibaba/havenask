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

#include <map>
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/util/VirtualAttrConvertor.h"
#include "suez/turing/expression/util/VirtualAttribute.h"
#include "turing_ops_util/variant/Tracer.h"

namespace autil {
namespace mem_pool {
class Pool;
} // namespace mem_pool
} // namespace autil
namespace indexlib {
namespace partition {
class PartitionReaderSnapshot;
} // namespace partition
} // namespace indexlib
namespace matchdoc {
class SubDocAccessor;
} // namespace matchdoc
namespace kmonitor {
class MetricsReporter;
}

namespace suez {
namespace turing {

class AttributeExpressionCreator;
class SuezCavaAllocator;

class ProviderBase {
public:
    ProviderBase(matchdoc::MatchDocAllocator *allocator,
                 autil::mem_pool::Pool *pool,
                 SuezCavaAllocator *cavaAllocator,
                 Tracer *requestTracer,
                 indexlib::partition::PartitionReaderSnapshot *partitionReaderSnapshot,
                 const KeyValueMap *kvpairs,
                 kmonitor::MetricsReporter *metricsReporter = nullptr);
    virtual ~ProviderBase();

private:
    ProviderBase(const ProviderBase &);
    ProviderBase &operator=(const ProviderBase &);

public:
    SuezCavaAllocator *getCavaAllocator() { return _cavaAllocator; }
    template <typename T>
    matchdoc::Reference<T> *
    declareVariableWithRawName(const std::string &variName,
                               uint8_t serializeLevel = SL_NONE,
                               bool needConstruct = matchdoc::ConstructTypeTraits<T>::NeedConstruct()) {
        matchdoc::Reference<T> *refer = NULL;
        if (!_isSubScope) {
            refer = _allocator->declareWithConstructFlagDefaultGroup<T>(variName, needConstruct, serializeLevel);
        } else {
            refer = _allocator->declareSubWithConstructFlagDefaultGroup<T>(variName, needConstruct, serializeLevel);
        }
        _declareVariable = refer != NULL;
        return refer;
    }
    // for ha3 old
    template <typename T>
    matchdoc::Reference<T> *declareVariable(const std::string &variName,
                                            uint8_t serializeLevel = SL_NONE,
                                            bool needConstruct = matchdoc::ConstructTypeTraits<T>::NeedConstruct()) {
        std::string newVariName = innerVariableName(variName);
        return declareVariableWithRawName<T>(newVariName, serializeLevel, needConstruct);
    }

    template <typename T>
    matchdoc::Reference<T> *declareSubVariable(const std::string &variName,
                                               uint8_t serializeLevel = SL_NONE,
                                               bool needConstruct = matchdoc::ConstructTypeTraits<T>::NeedConstruct()) {
        auto newVariName = innerVariableName(variName);
        auto refer = _allocator->declareSubWithConstructFlagDefaultGroup<T>(newVariName, needConstruct, serializeLevel);
        _declareVariable = refer != NULL;
        return refer;
    }

    template <typename T>
    matchdoc::Reference<T> *findVariableReference(const std::string &name) const {
        std::string newVariName = innerVariableName(name);
        return findVariableReferenceWithRawName<T>(newVariName);
    }

    matchdoc::ReferenceBase *findVariableReferenceWithoutType(const std::string &name) const {
        auto newVariName = innerVariableName(name);
        return _allocator->findReferenceWithoutType(newVariName);
    }

    template <typename T>
    matchdoc::Reference<T> *findVariableReferenceWithRawName(const std::string &name) const {
        return _allocator->findReference<T>(name);
    }

    matchdoc::ReferenceBase *findVariableReferenceWithRawNameWithoutType(const std::string &name) const {
        return _allocator->findReferenceWithoutType(name);
    }

    // add for be plugin convert
    template <typename T>
    matchdoc::Reference<T> *requireVariable(const std::string &name) const {
        matchdoc::Reference<T> *ref = findVariableReferenceWithRawName<T>(name);
        if (!ref) {
            ref = findVariableReference<T>(name);
        }
        return ref;
    }

    // add for be plugin convert
    matchdoc::ReferenceBase *requireVariableWithoutType(const std::string &name) const {
        matchdoc::ReferenceBase *ref = findVariableReferenceWithRawNameWithoutType(name);
        if (!ref) {
            ref = findVariableReferenceWithoutType(name);
        }
        return ref;
    }

    template <typename T>
    matchdoc::Reference<T> *requireAttribute(const std::string &attrName, bool needSerialize) {
        // in proxy or qrs
        if (!_partitionReaderSnapshot) {
            auto name = _convertor.nameToName(attrName);
            return dynamic_cast<matchdoc::Reference<T> *>(_allocator->findReferenceWithoutType(name));
        }
        auto attrExpr = doRequireAttributeWithoutType(attrName);
        if (!attrExpr) {
            return NULL;
        }
        auto *refBase = attrExpr->getReferenceBase();
        auto retRef = dynamic_cast<matchdoc::Reference<T> *>(refBase);
        if (retRef) {
            if (needSerialize) {
                refBase->setSerializeLevel(SL_CACHE);
            }
            _needEvaluateAttrs.push_back(attrExpr);
        }
        return retRef;
    }
    matchdoc::ReferenceBase *requireAttributeWithoutType(const std::string &attrName, bool needSerialize);

    const ExpressionVector &getNeedEvaluateAttrs() { return _needEvaluateAttrs; }

    void setAttributeEvaluated() {
        for (auto it = _needEvaluateAttrs.begin(); it != _needEvaluateAttrs.end(); it++) {
            (*it)->setEvaluated();
        }
    }
    matchdoc::SubDocAccessor *getSubDocAccessor() const { return _allocator->getSubDocAccessor(); }
    autil::mem_pool::Pool *getPool() { return _pool; }
    static std::string getInnerVariableName(const std::string &name) { return innerVariableName(name); }
    void setSubScope(bool isSubScope) { _isSubScope = isSubScope; }
    void unsetSubScope() { _isSubScope = false; }
    bool isSubScope() const { return _isSubScope; }
    matchdoc::MatchDocAllocator *getAllocator() { return _allocator; }
    indexlib::partition::PartitionReaderSnapshot *getPartitionReaderSnapshot() { return _partitionReaderSnapshot; }
    std::string getMatchDocFieldGroup() { return _allocator->getDefaultGroupName(); }
    void setMatchDocFieldGroup(const std::string &groupName) { return _allocator->setDefaultGroupName(groupName); }
    const KeyValueMap &getKVMap() const {
        static KeyValueMap EMPTY_KVMAP;
        if (!_kvpairs) {
            return EMPTY_KVMAP;
        }
        return *_kvpairs;
    }
    const std::string &getKVPairValue(const std::string &key) const {
        static std::string EMPTY_STRING = "";
        if (!_kvpairs) {
            return EMPTY_STRING;
        }
        std::map<std::string, std::string>::const_iterator it = _kvpairs->find(key);
        if (it == _kvpairs->end()) {
            return EMPTY_STRING;
        }
        return it->second;
    }

    kmonitor::MetricsReporter *getMetricsReporter() { return _metricsReporter; }
    matchdoc::Reference<Tracer> *getTracerRefer() { return _traceRefer; }
    Tracer *getRequestTracer() { return _requestTracer; }
    void setRequestTracer(Tracer *tracer) { _requestTracer = tracer; }
    void setAttributeExpressionCreator(AttributeExpressionCreator *creator) { _attributeExpressionCreator = creator; }
    void initVirtualAttrs(const VirtualAttributes &virtualAttributes) {
        _convertor.initVirtualAttrs(virtualAttributes);
    }
    bool hasDeclareVariable() { return _declareVariable; }

    void setupTraceRefer(int32_t level) {
        _rankTraceLevel = level;
        if (_rankTraceLevel < ISEARCH_TRACE_NONE) {
            _traceRefer = _allocator->declare<Tracer>(RANK_TRACER_NAME, SL_CACHE);
        }
    }

    int32_t getTracerLevel() { return _rankTraceLevel; }

    void prepareTracer(matchdoc::MatchDoc matchDoc) {
        if (unlikely(_traceRefer != NULL)) {
            Tracer *tracer = _traceRefer->getPointer(matchDoc);
            tracer->setLevel(_rankTraceLevel);
        }
    }
    void batchPrepareTracer(matchdoc::MatchDoc *matchDocs, uint32_t matchDocCount) {
        if (unlikely(_traceRefer != NULL)) {
            for (uint32_t i = 0; i < matchDocCount; i++) {
                Tracer *tracer = _traceRefer->getPointer(matchDocs[i]);
                tracer->setLevel(_rankTraceLevel);
            }
        }
    }
    matchdoc::MatchDocAllocator *setAllocator(matchdoc::MatchDocAllocator *allocator) {
        auto tmp = _allocator;
        _allocator = allocator;
        return tmp;
    }

protected:
    AttributeExpression *doRequireAttributeWithoutType(const std::string &attrName);

    void addNeedEvaluateExpression(AttributeExpression *expr) {
        ExpressionType exprType = expr->getExpressionType();
        if (exprType != ET_RANK) {
            _needEvaluateAttrs.push_back(expr);
        }
    }
    void clearNeedEvaluateExpression() { _needEvaluateAttrs.clear(); }

private:
    static std::string innerVariableName(const std::string &name) {
        if (autil::StringUtil::startsWith(name, BUILD_IN_REFERENCE_PREFIX)) {
            return name;
        }
        return PROVIDER_VAR_NAME_PREFIX + name;
    }

protected:
    matchdoc::MatchDocAllocator *_allocator = nullptr;
    autil::mem_pool::Pool *_pool = nullptr;
    SuezCavaAllocator *_cavaAllocator = nullptr;
    indexlib::partition::PartitionReaderSnapshot *_partitionReaderSnapshot = nullptr;
    bool _isSubScope;
    const KeyValueMap *_kvpairs = nullptr;
    kmonitor::MetricsReporter *_metricsReporter = nullptr;
    AttributeExpressionCreator *_attributeExpressionCreator = nullptr;
    matchdoc::Reference<Tracer> *_traceRefer = nullptr;
    Tracer *_requestTracer = nullptr;
    int32_t _rankTraceLevel;
    ExpressionVector _needEvaluateAttrs;
    bool _declareVariable;
    VirtualAttrConvertor _convertor;

private:
    AUTIL_LOG_DECLARE();
};

SUEZ_TYPEDEF_PTR(ProviderBase);

} // namespace turing
} // namespace suez

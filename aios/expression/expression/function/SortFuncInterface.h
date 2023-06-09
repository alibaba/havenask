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
#ifndef ISEARCH_EXPRESSION_SORTFUNCINTERFACE_H
#define ISEARCH_EXPRESSION_SORTFUNCINTERFACE_H

#include "expression/common.h"
#include "expression/function/FunctionInterface.h"
#include "expression/function/FunctionCreator.h"
#include "matchdoc/MatchDoc.h"

namespace expression {

template <typename T>
struct SortCmp {
    SortCmp(bool asc, matchdoc::Reference<T> *ref)
        : _asc(asc)
        , _ref(ref)
    {}
public:
    bool operator()(matchdoc::MatchDoc lft, matchdoc::MatchDoc rht) {
        const T &left = _ref->get(lft);
        const T &right = _ref->get(rht);
        return _asc ? left <= right : left >= right;
    }
private:
    bool _asc;
    matchdoc::Reference<T> *_ref;
};

class SortFuncInterface : public FunctionInterfaceTyped<void>
{
public:
    SortFuncInterface(bool asc, const std::string &sortFieldName)
        : _asc(asc)
        , _sortFieldName(sortFieldName)
        , _sortRef(NULL)
    {}
    ~SortFuncInterface() {}
private:
    SortFuncInterface(const SortFuncInterface &);
    SortFuncInterface& operator=(const SortFuncInterface &);
public:
    /* override */ bool beginRequest(FunctionProvider *provider) {
        _sortRef = provider->requireAttribute(_sortFieldName);
        if (!_sortRef) {
            AUTIL_LOG(WARN, "requireAttribute %s faield in %s", _sortFieldName.c_str(),
                      provider->getLocation() == FL_SEARCHER ? "searcher" : "proxy");
            return false;
        }
        matchdoc::ValueType vt = _sortRef->getValueType();
        if (vt.isMultiValue() || !vt.isBuiltInType()) {
            return false;
        }
        if (provider->getLocation() == FL_SEARCHER) {
            provider->addSortMeta(_sortFieldName, _sortRef, _asc);
        }
        return true;
    }
    /* override */ void evaluate(const matchdoc::MatchDoc &matchDoc) {
    }
    /* override */ void batchEvaluate(matchdoc::MatchDoc *matchDocs, uint32_t count) {
        matchdoc::ValueType vt = _sortRef->getValueType();
        matchdoc::BuiltinType bt = vt.getBuiltinType();

#define SORT_MACRO(bt_type)                                             \
        case bt_type:                                                   \
        {                                                               \
            typedef matchdoc::BuiltinType2CppType<bt_type>::CppType T; \
            matchdoc::Reference<T> *typedRef = dynamic_cast<matchdoc::Reference<T> *>(_sortRef); \
            if (!typedRef) {                                            \
                return;                                                 \
            }                                                           \
            std::sort(matchDocs, matchDocs + count, SortCmp<T>(_asc, typedRef)); \
        }                                                               \
        break
        
        switch (bt) {
            BUILTIN_TYPE_MACRO_HELPER(SORT_MACRO);            
        default:
            return;
        }
    }
private:
    bool _asc;
    std::string _sortFieldName;
    matchdoc::ReferenceBase *_sortRef;
private:
    AUTIL_LOG_DECLARE();
};

class SortFuncInterfaceCreator : public FunctionCreator {
    TEMPLATE_FUNCTION_DESCRIBE_DEFINE(void, "sort", 1,
            FT_NORMAL, FUNC_ACTION_SCOPE_ADAPTER, FUNC_BATCH_CUSTOM_MODE);
    FUNCTION_OVERRIDE_FUNC();
private:
    AUTIL_LOG_DECLARE();
};

}

#endif //ISEARCH_EXPRESSION_SORTFUNCINTERFACE_H

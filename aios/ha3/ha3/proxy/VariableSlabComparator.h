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

#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"

namespace isearch {
namespace proxy {

class VariableSlabComparator;

typedef std::shared_ptr<VariableSlabComparator> VariableSlabComparatorPtr;

class VariableSlabComparator
{
public:
    VariableSlabComparator();
    virtual ~VariableSlabComparator();
public:
    virtual bool compare(const matchdoc::MatchDoc a, const matchdoc::MatchDoc b) = 0;
public:
    static VariableSlabComparatorPtr createComparator(const matchdoc::ReferenceBase *ref);
private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
class VariableSlabComparatorTyped : public VariableSlabComparator
{
public:
    VariableSlabComparatorTyped(const matchdoc::Reference<T> *ref) {
        _ref = ref;
    }
    ~VariableSlabComparatorTyped() {}
public:
    bool compare(const matchdoc::MatchDoc a, const matchdoc::MatchDoc b) override {
        return _ref->getReference(b) < _ref->getReference(a);
    }
private:
    const matchdoc::Reference<T> *_ref;
private:
    AUTIL_LOG_DECLARE();
};

template <>
class VariableSlabComparatorTyped<std::string> : public VariableSlabComparator
{
public:
    VariableSlabComparatorTyped(const matchdoc::Reference<std::string> *ref) {
        _ref = ref;
    }
    ~VariableSlabComparatorTyped() {}
public:
    bool compare(const matchdoc::MatchDoc a, const matchdoc::MatchDoc b) override {
        return _ref->getReference(a).compare(_ref->getReference(b)) < 0;
    }
private:
    const matchdoc::Reference<std::string> *_ref;
private:
    AUTIL_LOG_DECLARE();
};

class VariableSlabCompImpl {
public:
    VariableSlabCompImpl(VariableSlabComparator *comp) {
        _comp = comp;
    }
public:
    inline bool operator()(matchdoc::MatchDoc a, matchdoc::MatchDoc b) {
        return _comp->compare(a, b);
    }
private:
    VariableSlabComparator *_comp;
};

} // namespace proxy
} // namespace isearch


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

#include <cmath>
#include <map>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/MultiValueCreator.h"
#include "autil/legacy/jsonizable.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/toolkit/Schema.h"

namespace matchdoc {

class MatchDoc;
class ReferenceBase;
struct Doc;

class MatchDocHelper {
public:
    static bool prepare(const std::string &docSchemaStr,
                        const std::vector<std::string> &docValStrVec,
                        MatchDocAllocatorPtr &allocator,
                        std::vector<MatchDoc> &docs,
                        const std::vector<int32_t> &docIds = {});
    static bool prepareFromJsonFile(const std::string &absPath,
                                    MatchDocAllocatorPtr &allocator,
                                    std::vector<MatchDoc> &docs,
                                    const std::vector<int32_t> &docIds = {});
    static bool prepareFromJson(const std::string &content,
                                MatchDocAllocatorPtr &allocator,
                                std::vector<MatchDoc> &docs,
                                const std::vector<int32_t> &docIds = {});

    template <typename T>
    static bool extend(MatchDocAllocatorPtr &allocator,
                       const std::vector<MatchDoc> &docs,
                       const std::string &fieldName,
                       const std::vector<T> &values);

    template <typename T>
    static bool extendMultiVal(MatchDocAllocatorPtr &allocator,
                               const std::vector<MatchDoc> &docs,
                               const std::string &fieldName,
                               const std::vector<std::vector<T>> &values);

    template <typename T>
    static bool checkField(matchdoc::MatchDocAllocatorPtr &allocator,
                           const std::vector<matchdoc::MatchDoc> &matchdocs,
                           const std::string &fieldName,
                           const std::vector<T> &expectValues);
    template <typename T>
    static bool checkMultiValField(matchdoc::MatchDocAllocatorPtr &allocator,
                                   const std::vector<matchdoc::MatchDoc> &matchdocs,
                                   const std::string &name,
                                   const std::vector<std::vector<T>> &values);

    static bool checkField(MatchDocAllocatorPtr &leftAlloc,
                           const std::vector<MatchDoc> &leftDocs,
                           MatchDocAllocatorPtr &rightAlloc,
                           const std::vector<MatchDoc> &rightDocs,
                           const std::string &field);

    static bool check(MatchDocAllocatorPtr &leftAlloc,
                      const std::vector<MatchDoc> &leftDocs,
                      MatchDocAllocatorPtr &rightAlloc,
                      const std::vector<MatchDoc> &rightDocs,
                      std::vector<std::string> fields = {});

    static bool checkDoc(const std::string &expectVal, MatchDocAllocatorPtr &allocator, MatchDoc doc);

    static std::vector<std::string> printAsDocs(const MatchDocAllocatorPtr &allocator,
                                                const std::vector<MatchDoc> &docs,
                                                std::vector<std::string> fields = {});

    static std::string print(const MatchDocAllocatorPtr &allocator,
                             const std::vector<MatchDoc> &docs,
                             std::vector<std::string> fields = {});

private:
    static bool fromRawString(const std::string &docSchemaStr, const std::vector<std::string> &docValStr, Doc &doc);
    static bool doPrepare(const Doc &doc,
                          MatchDocAllocatorPtr &allocator,
                          std::vector<MatchDoc> &docs,
                          const std::vector<int32_t> &docIds = {});
    static bool parseValue(const std::vector<std::string> &docValStr,
                           std::map<std::string, std::vector<std::string>> &fieldName2valVec);

    static std::string
    printDoc(const std::vector<ReferenceBase *> &refs, const std::vector<std::string> &names, MatchDoc doc);

    template <typename T>
    static std::string refValueToString(const std::string &name, ReferenceBase *ref, MatchDoc doc);

private:
    static const double PRECISION;

private:
    AUTIL_LOG_DECLARE();
};

template <typename T>
bool MatchDocHelper::extend(MatchDocAllocatorPtr &allocator,
                            const std::vector<MatchDoc> &docs,
                            const std::string &fieldName,
                            const std::vector<T> &values) {
    auto ref = allocator->declare<T>(fieldName);
    allocator->extend();
    if (values.size() != docs.size()) {
        AUTIL_LOG(WARN, "values size:[%zu] != docs size:[%zu]", values.size(), docs.size());
        return false;
    }
    for (size_t i = 0; i < docs.size(); ++i) {
        auto value = values[i];
        auto doc = docs[i];
        ref->set(doc, value);
    }
    return true;
}

template <>
inline bool MatchDocHelper::extend(MatchDocAllocatorPtr &allocator,
                                   const std::vector<MatchDoc> &docs,
                                   const std::string &fieldName,
                                   const std::vector<std::string> &values) {
    auto ref = allocator->declare<autil::MultiChar>(fieldName);
    allocator->extend();
    if (values.size() != docs.size()) {
        AUTIL_LOG(WARN, "values size:[%zu] != docs size:[%zu]", values.size(), docs.size());
        return false;
    }
    for (size_t i = 0; i < docs.size(); ++i) {
        auto value = values[i];
        auto doc = docs[i];
        auto buffer =
            autil::MultiValueCreator::createMultiValueBuffer(value.data(), value.size(), allocator->getPool());
        ref->getReference(doc).init(buffer);
    }
    return true;
}

template <typename T>
bool MatchDocHelper::extendMultiVal(MatchDocAllocatorPtr &allocator,
                                    const std::vector<MatchDoc> &docs,
                                    const std::string &fieldName,
                                    const std::vector<std::vector<T>> &values) {
    auto ref = allocator->declare<autil::MultiValueType<T>>(fieldName);
    allocator->extend();
    if (values.size() != docs.size()) {
        AUTIL_LOG(WARN, "values size:[%zu] != docs size:[%zu]", values.size(), docs.size());
        return false;
    }
    for (size_t i = 0; i < docs.size(); ++i) {
        auto value = values[i];

        auto doc = docs[i];
        auto buffer = autil::MultiValueCreator::createMultiValueBuffer(values[i], allocator->getPool());
        ref->getReference(doc).init(buffer);
    }
    return true;
}

template <>
inline bool MatchDocHelper::extendMultiVal(MatchDocAllocatorPtr &allocator,
                                           const std::vector<MatchDoc> &docs,
                                           const std::string &fieldName,
                                           const std::vector<std::vector<std::string>> &values) {
    auto ref = allocator->declare<autil::MultiString>(fieldName);
    allocator->extend();
    if (values.size() != docs.size()) {
        AUTIL_LOG(WARN, "values size:[%zu] != docs size:[%zu]", values.size(), docs.size());
        return false;
    }
    for (size_t i = 0; i < docs.size(); ++i) {
        auto doc = docs[i];
        auto buffer = autil::MultiValueCreator::createMultiValueBuffer(values[i], allocator->getPool());
        ref->getReference(doc).init(buffer);
    }
    return true;
}

template <typename T>
bool MatchDocHelper::checkField(matchdoc::MatchDocAllocatorPtr &allocator,
                                const std::vector<matchdoc::MatchDoc> &matchdocs,
                                const std::string &name,
                                const std::vector<T> &values) {
    if (values.size() != matchdocs.size()) {
        AUTIL_LOG(WARN, "value size:[%zu] != matchdoc size:[%zu]", matchdocs.size(), values.size());
        return false;
    }
    matchdoc::Reference<T> *ref = allocator->findReference<T>(name);
    if (!ref) {
        AUTIL_LOG(WARN, "get ref:[%s] failed", name.c_str());
        return false;
    }
    for (size_t i = 0; i < values.size(); i++) {
        const T &actual = ref->get(matchdocs[i]);
        if (actual != values[i]) {
            return false;
        }
    }
    return true;
}

template <>
inline bool MatchDocHelper::checkField(matchdoc::MatchDocAllocatorPtr &allocator,
                                       const std::vector<matchdoc::MatchDoc> &matchdocs,
                                       const std::string &name,
                                       const std::vector<float> &values) {
    if (values.size() != matchdocs.size()) {
        AUTIL_LOG(WARN, "value size:[%zu] != matchdoc size:[%zu]", matchdocs.size(), values.size());
        return false;
    }
    matchdoc::Reference<float> *ref = allocator->findReference<float>(name);
    if (!ref) {
        AUTIL_LOG(WARN, "get ref:[%s] failed", name.c_str());
        return false;
    }
    for (size_t i = 0; i < values.size(); i++) {
        const float &actual = ref->get(matchdocs[i]);
        if (fabs(actual - values[i]) > PRECISION) {
            AUTIL_LOG(WARN, "actual:[%f] != expect:[%f]", actual, values[i]);
            return false;
        }
    }
    return true;
}

template <>
inline bool MatchDocHelper::checkField(matchdoc::MatchDocAllocatorPtr &allocator,
                                       const std::vector<matchdoc::MatchDoc> &matchdocs,
                                       const std::string &name,
                                       const std::vector<double> &values) {
    if (values.size() != matchdocs.size()) {
        AUTIL_LOG(WARN, "value size:[%zu] != matchdoc size:[%zu]", matchdocs.size(), values.size());
        return false;
    }
    matchdoc::Reference<double> *ref = allocator->findReference<double>(name);
    if (!ref) {
        AUTIL_LOG(WARN, "get ref:[%s] failed", name.c_str());
        return false;
    }
    for (size_t i = 0; i < values.size(); i++) {
        const double &actual = ref->get(matchdocs[i]);
        if (fabs(actual - values[i]) > PRECISION) {
            AUTIL_LOG(WARN, "actual:[%f] != expect:[%f]", actual, values[i]);
            return false;
        }
    }
    return true;
}

template <>
inline bool MatchDocHelper::checkField(matchdoc::MatchDocAllocatorPtr &allocator,
                                       const std::vector<matchdoc::MatchDoc> &matchdocs,
                                       const std::string &name,
                                       const std::vector<std::string> &values) {
    if (values.size() != matchdocs.size()) {
        AUTIL_LOG(WARN, "value size:[%zu] != matchdoc size:[%zu]", matchdocs.size(), values.size());
        return false;
    }
    matchdoc::Reference<autil::MultiChar> *ref = allocator->findReference<autil::MultiChar>(name);
    if (!ref) {
        AUTIL_LOG(WARN, "get ref:[%s] failed", name.c_str());
        return false;
    }
    for (size_t i = 0; i < values.size(); i++) {
        const autil::MultiChar &actual = ref->get(matchdocs[i]);
        std::string val(actual.data(), actual.size());
        if (val != values[i]) {
            AUTIL_LOG(WARN, "actual:[%s] != expect:[%s]", val.c_str(), values[i].c_str());
            return false;
        }
    }
    return true;
}

template <typename T>
bool MatchDocHelper::checkMultiValField(matchdoc::MatchDocAllocatorPtr &allocator,
                                        const std::vector<matchdoc::MatchDoc> &matchdocs,
                                        const std::string &name,
                                        const std::vector<std::vector<T>> &values) {
    typedef autil::MultiValueType<T> Type;
    if (values.size() != matchdocs.size()) {
        AUTIL_LOG(WARN, "value size:[%zu] != matchdoc size:[%zu]", matchdocs.size(), values.size());
        return false;
    }
    matchdoc::Reference<Type> *ref = allocator->findReference<Type>(name);
    if (!ref) {
        AUTIL_LOG(WARN, "get ref:[%s] failed", name.c_str());
        return false;
    }
    for (size_t i = 0; i < matchdocs.size(); i++) {
        const std::vector<T> &expected = values[i];
        Type actual = ref->get(matchdocs[i]);
        if (expected.size() != actual.size()) {
            AUTIL_LOG(WARN, "#[%lu] actual size:[%d] != expect size:[%zu]", i + 1, actual.size(), expected.size());
            return false;
        }
        for (size_t j = 0; j < expected.size(); j++) {
            if (expected[j] != actual[j]) {
                return false;
            }
        }
    }
    return true;
}

template <>
inline bool MatchDocHelper::checkMultiValField(matchdoc::MatchDocAllocatorPtr &allocator,
                                               const std::vector<matchdoc::MatchDoc> &matchdocs,
                                               const std::string &name,
                                               const std::vector<std::vector<float>> &values) {
    typedef autil::MultiValueType<float> Type;
    if (values.size() != matchdocs.size()) {
        AUTIL_LOG(WARN, "value size:[%zu] != matchdoc size:[%zu]", matchdocs.size(), values.size());
        return false;
    }
    matchdoc::Reference<Type> *ref = allocator->findReference<Type>(name);
    if (!ref) {
        AUTIL_LOG(WARN, "get ref:[%s] failed", name.c_str());
        return false;
    }
    for (size_t i = 0; i < matchdocs.size(); i++) {
        const std::vector<float> &expected = values[i];
        Type actual = ref->get(matchdocs[i]);
        if (expected.size() != actual.size()) {
            AUTIL_LOG(WARN, "#[%zu] actual size:[%u] != expect size:[%zu]", i + 1, actual.size(), expected.size());
            return false;
        }
        for (size_t j = 0; j < expected.size(); j++) {
            if (fabs(expected[j] - actual[j]) > PRECISION) {
                AUTIL_LOG(WARN, "actual:[%f] != expect:[%f]", actual[j], expected[j]);
                return false;
            }
        }
    }
    return true;
}

template <>
inline bool MatchDocHelper::checkMultiValField(matchdoc::MatchDocAllocatorPtr &allocator,
                                               const std::vector<matchdoc::MatchDoc> &matchdocs,
                                               const std::string &name,
                                               const std::vector<std::vector<double>> &values) {
    typedef autil::MultiValueType<double> Type;
    if (values.size() != matchdocs.size()) {
        AUTIL_LOG(WARN, "value size:[%zu] != matchdoc size:[%zu]", matchdocs.size(), values.size());
        return false;
    }
    matchdoc::Reference<Type> *ref = allocator->findReference<Type>(name);
    if (!ref) {
        AUTIL_LOG(WARN, "get ref:[%s] failed", name.c_str());
        return false;
    }
    for (size_t i = 0; i < matchdocs.size(); i++) {
        const std::vector<double> &expected = values[i];
        Type actual = ref->get(matchdocs[i]);
        if (expected.size() != actual.size()) {
            AUTIL_LOG(WARN, "#[%zu] actual size:[%u] != expect size:[%zu]", i + 1, actual.size(), expected.size());
            return false;
        }
        for (size_t j = 0; j < expected.size(); j++) {
            if (fabs(expected[j] - actual[j]) > PRECISION) {
                AUTIL_LOG(WARN, "actual:[%f] != expect:[%f]", actual[j], expected[j]);
                return false;
            }
        }
    }
    return true;
}

template <>
inline bool MatchDocHelper::checkMultiValField(matchdoc::MatchDocAllocatorPtr &allocator,
                                               const std::vector<matchdoc::MatchDoc> &matchdocs,
                                               const std::string &name,
                                               const std::vector<std::vector<std::string>> &values) {
    if (values.size() != matchdocs.size()) {
        AUTIL_LOG(WARN, "value size:[%zu] != matchdoc size:[%zu]", matchdocs.size(), values.size());
        return false;
    }
    matchdoc::Reference<autil::MultiString> *ref = allocator->findReference<autil::MultiString>(name);
    if (!ref) {
        AUTIL_LOG(WARN, "get ref:[%s] failed", name.c_str());
        return false;
    }
    for (size_t i = 0; i < matchdocs.size(); i++) {
        const std::vector<std::string> &expected = values[i];
        const autil::MultiString &actual = ref->get(matchdocs[i]);
        if (expected.size() != actual.size()) {
            AUTIL_LOG(WARN, "#[%zu] actual size:[%u] != expect size:[%zu]", i + 1, actual.size(), expected.size());
            return false;
        }
        for (size_t j = 0; j < expected.size(); j++) {
            std::string actualStr(actual[j].data(), actual[j].size());
            if (expected[j] != actualStr) {
                AUTIL_LOG(WARN, "actual:[%s] != expect:[%s]", actualStr.c_str(), expected[j].c_str());
                return false;
            }
        }
    }
    return true;
}

} // namespace matchdoc

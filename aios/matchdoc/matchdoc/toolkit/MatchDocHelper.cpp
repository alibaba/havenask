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
#include "matchdoc/toolkit/MatchDocHelper.h"

#include <ext/alloc_traits.h>
#include <fstream>
#include <memory>
#include <set>
#include <stddef.h>
#include <utility>

#include "alog/Logger.h"
#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/jsonizable.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/ValueType.h"
#include "matchdoc/toolkit/Schema.h"

namespace matchdoc {

using namespace std;
using namespace autil;
using namespace matchdoc;

const double MatchDocHelper::PRECISION = 0.000001;

AUTIL_LOG_SETUP(turing, MatchDocHelper);

bool MatchDocHelper::prepare(const std::string &schemaStr,
                             const vector<std::string> &docVals,
                             MatchDocAllocatorPtr &allocator,
                             vector<MatchDoc> &docs,
                             const std::vector<int32_t> &docIds) {
    if (!docIds.empty() && docIds.size() != docVals.size()) {
        AUTIL_LOG(ERROR, "docIds size[%zu] differs from docVals[%zu]", docIds.size(), docVals.size());
        return false;
    }

    if (!allocator) {
        AUTIL_LOG(INFO, "allocator is nullptr");
        return false;
    }

    Doc doc;
    if (!fromRawString(schemaStr, docVals, doc)) {
        return false;
    }

    return doPrepare(doc, allocator, docs, docIds);
}

bool MatchDocHelper::prepareFromJson(const string &content,
                                     MatchDocAllocatorPtr &allocator,
                                     vector<MatchDoc> &docs,
                                     const vector<int32_t> &docIds) {
    Doc doc;
    try {
        FromJsonString(doc, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "failed to deserialize, error[%s], content[%s].", e.what(), content.c_str());
        return false;
    }
    if (!doPrepare(doc, allocator, docs, docIds)) {
        return false;
    }
    return true;
}

bool MatchDocHelper::prepareFromJsonFile(const string &absPath,
                                         MatchDocAllocatorPtr &allocator,
                                         vector<MatchDoc> &docs,
                                         const vector<int32_t> &docIds) {
    ifstream file(absPath);
    string content((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    file.close();

    Doc doc;
    try {
        FromJsonString(doc, content);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "failed to deserialize, error[%s], content[%s].", e.what(), content.c_str());
        return false;
    }

    if (!doPrepare(doc, allocator, docs, docIds)) {
        return false;
    }
    return true;
}

bool MatchDocHelper::fromRawString(const string &schemaStr, const vector<string> &docValStr, Doc &doc) {
    map<string, vector<string>> fieldName2val;
    if (!parseValue(docValStr, fieldName2val)) {
        return false;
    }
    vector<string> fieldSchemaVec = StringUtil::split(schemaStr, kFieldSchemaSep);
    for (const auto &fieldSchemaStr : fieldSchemaVec) {
        vector<string> attrs = StringUtil::split(fieldSchemaStr, kAttrSchemaSep);
        if (attrs.size() != 3u && attrs.size() != 2u) {
            AUTIL_LOG(ERROR,
                      "unexpected attrs[%s] attribute number[%zu], expected size[2, 3]",
                      fieldSchemaStr.c_str(),
                      attrs.size());
            return false;
        }
        StringUtil::trim(attrs[0]);
        StringUtil::trim(attrs[1]);

        Field field;
        if (attrs[0].empty()) {
            AUTIL_LOG(ERROR, "attrs[0] in field[%s] is empty", fieldSchemaStr.c_str());
            return false;
        }
        field._name = attrs[0];
        if (attrs[1].empty()) {
            AUTIL_LOG(ERROR, "attrs[1] in field[%s] is empty", fieldSchemaStr.c_str());
            return false;
        }
        field._type = fromString(attrs[1]);
        if (attrs.size() == 3u) {
            StringUtil::trim(attrs[2]);
            if (!StringUtil::fromString(attrs[2], field._isMulti)) {
                AUTIL_LOG(ERROR, "failed to cast [%s] to boolean", attrs[2].c_str());
                return false;
            }
        } else {
            field._isMulti = false;
        }
        doc._fields.push_back(field);
    }

    for (auto &field : doc._fields) {
        auto itr = fieldName2val.find(field._name);
        if (itr != fieldName2val.end()) {
            field._fieldVals.swap(itr->second);
        }
        field._multiValSep = kMultiValSep;
    }
    doc._name = "build_from_str";
    return true;
}

bool MatchDocHelper::parseValue(const vector<string> &docValStr, map<string, vector<string>> &fieldName2val) {
    set<string> fieldSet;
    for (size_t i = 0; i < docValStr.size(); ++i) {
        vector<string> fieldVals = StringUtil::split(docValStr[i], kFieldValSep);
        if (i != 0 && fieldSet.size() != fieldVals.size()) {
            AUTIL_LOG(ERROR, "field size error[%s]", docValStr[i].c_str());
            return false;
        }
        for (const auto &field : fieldVals) {
            vector<string> kv = StringUtil::split(field, "=");
            if (kv.size() != 2 || (kv.size() == 2 && (kv[0].empty() || kv[1].empty()))) {
                AUTIL_LOG(ERROR, "format error: expect[$(key)=$(value)], actual[%s]", field.c_str());
                return false;
            }
            StringUtil::trim(kv[0]);
            StringUtil::trim(kv[1]);
            if (i == 0) {
                if (!fieldSet.insert(kv[0]).second) {
                    AUTIL_LOG(ERROR, "field[%s] appears at leat two times", kv[0].c_str());
                    return false;
                }
            } else {
                if (fieldSet.find(kv[0]) == fieldSet.end()) {
                    AUTIL_LOG(ERROR, "field[%s] does not appear in each doc", kv[0].c_str());
                    return false;
                }
            }
            fieldName2val[kv[0]].push_back(kv[1]);
        }
    }
    return true;
}

bool MatchDocHelper::doPrepare(const Doc &doc,
                               MatchDocAllocatorPtr &allocator,
                               vector<MatchDoc> &docs,
                               const std::vector<int32_t> &docIds) {
#ifdef __ENABLE_SCHEMA_DEBUG__
    string debug;
    try {
        debug = autil::legacy::ToJsonString(doc);
    } catch (const autil::legacy::ExceptionBase &e) {
        AUTIL_LOG(ERROR, "failed to transfer, exception[%s]", e.what());
        return false;
    }
    AUTIL_LOG(INFO, "schema debug: [%s]", debug.c_str());
#endif

    if (!allocator) {
        AUTIL_LOG(ERROR, "allocator is nullptr in schema[%s]", doc._name.c_str());
        return false;
    }
    if (docIds.empty()) {
        size_t size = 0u;
        if (!doc._fields.empty()) {
            size = doc._fields[0]._fieldVals.size();
        }
        for (size_t i = 0; i < size; ++i) {
            docs.push_back(allocator->allocate());
        }
    } else {
        for (size_t i = 0; i < docIds.size(); ++i) {
            docs.push_back(allocator->allocate(docIds[i]));
        }
    }
    for (const auto &field : doc._fields) {
        switch (field._type) {
#define CASE(bt)                                                                                                       \
    case (bt): {                                                                                                       \
        typedef MatchDocBuiltinType2CppType<bt, false>::CppType Type;                                                  \
        if (field._isMulti) {                                                                                          \
            vector<vector<Type>> typedVals(field._fieldVals.size());                                                   \
            int32_t i = 0;                                                                                             \
            for (const auto &rawVal : field._fieldVals) {                                                              \
                StringUtil::fromString(rawVal, typedVals[i++], field._multiValSep);                                    \
            }                                                                                                          \
            extendMultiVal(allocator, docs, field._name, typedVals);                                                   \
        } else {                                                                                                       \
            vector<Type> typedVals;                                                                                    \
            StringUtil::fromString(field._fieldVals, typedVals);                                                       \
            extend(allocator, docs, field._name, typedVals);                                                           \
        }                                                                                                              \
    } break;
            NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE);
        case (bt_string): {
            if (field._isMulti) {
                vector<vector<string>> typedVals(field._fieldVals.size());
                int32_t i = 0;
                for (const auto &rawVal : field._fieldVals) {
                    typedVals[i++] = StringUtil::split(rawVal, field._multiValSep);
                }
                extendMultiVal(allocator, docs, field._name, typedVals);
            } else {
                vector<string> typedVals;
                for (const auto &rawVal : field._fieldVals) {
                    // string val(rawVal.data(), rawVal.size());
                    typedVals.emplace_back(rawVal.data(), rawVal.size());
                }
                extend(allocator, docs, field._name, typedVals);
            }
        } break;
        default:
            AUTIL_LOG(ERROR, "unknown match type:[%d]", field._type);
            return false;
#undef CASE
        }
    }
    return true;
}

bool MatchDocHelper::checkField(MatchDocAllocatorPtr &leftAlloc,
                                const std::vector<MatchDoc> &leftDocs,
                                MatchDocAllocatorPtr &rightAlloc,
                                const std::vector<MatchDoc> &rightDocs,
                                const std::string &field) {
    if (leftAlloc == nullptr) {
        AUTIL_LOG(WARN, "left allocator is null");
        return false;
    }
    if (rightAlloc == nullptr) {
        AUTIL_LOG(WARN, "right allocator is null");
        return false;
    }
    ReferenceBase *ref = rightAlloc->findReferenceWithoutType(field);
    if (!ref) {
        AUTIL_LOG(WARN, "find ref:[%s] failed", field.c_str());
        return false;
    }

    bool result = false;
    BuiltinType type = ref->getValueType().getBuiltinType();
    bool isMulti = ref->getValueType().isMultiValue();
    switch (type) {
#define CASE(bt)                                                                                                       \
    case (bt): {                                                                                                       \
        typedef MatchDocBuiltinType2CppType<bt, false>::CppType Type;                                                  \
        if (isMulti) {                                                                                                 \
            vector<vector<Type>> typedVals(rightDocs.size());                                                          \
            int32_t i = 0;                                                                                             \
            Reference<autil::MultiValueType<Type>> *typedRef =                                                         \
                dynamic_cast<Reference<autil::MultiValueType<Type>> *>(ref);                                           \
            for (auto doc : rightDocs) {                                                                               \
                const auto &rightDocVals = typedRef->get(doc);                                                         \
                for (size_t j = 0; j < rightDocVals.size(); ++j) {                                                     \
                    typedVals[i].push_back(rightDocVals[j]);                                                           \
                }                                                                                                      \
                ++i;                                                                                                   \
            }                                                                                                          \
            result = checkMultiValField(leftAlloc, leftDocs, field, typedVals);                                        \
        } else {                                                                                                       \
            Reference<Type> *typedRef = dynamic_cast<Reference<Type> *>(ref);                                          \
            vector<Type> typedVals;                                                                                    \
            for (auto doc : rightDocs) {                                                                               \
                typedVals.push_back(typedRef->get(doc));                                                               \
            }                                                                                                          \
            result = checkField(leftAlloc, leftDocs, field, typedVals);                                                \
        }                                                                                                              \
    } break;
        NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE);
    case (bt_string): {
        if (isMulti) {
            vector<vector<string>> typedVals(rightDocs.size());
            Reference<autil::MultiString> *typedRef = dynamic_cast<Reference<autil::MultiString> *>(ref);
            int32_t i = 0;
            for (auto doc : rightDocs) {
                const autil::MultiString &vals = typedRef->get(doc);
                for (size_t j = 0; j < vals.size(); ++j) {
                    typedVals[i].emplace_back(vals[j].data(), vals[j].size());
                }
                ++i;
            }
            result = checkMultiValField(leftAlloc, leftDocs, field, typedVals);
        } else {
            vector<string> typedVals;
            if (!ref->getValueType().isStdType()) {
                Reference<autil::MultiChar> *typedRef = dynamic_cast<Reference<autil::MultiChar> *>(ref);
                for (auto doc : rightDocs) {
                    const autil::MultiChar &val = typedRef->get(doc);
                    typedVals.emplace_back(val.data(), val.size());
                }
            } else {
                Reference<string> *typedRef = dynamic_cast<Reference<string> *>(ref);
                for (auto doc : rightDocs) {
                    string val = typedRef->get(doc);
                    typedVals.emplace_back(val.data(), val.size());
                }
            }
            result = checkField(leftAlloc, leftDocs, field, typedVals);
        }
    } break;
    default:
        AUTIL_LOG(ERROR, "unknown match type:[%d]", type);
        return false;
#undef CASE
    }
    return result;
}

bool MatchDocHelper::check(MatchDocAllocatorPtr &leftAlloc,
                           const std::vector<MatchDoc> &leftDocs,
                           MatchDocAllocatorPtr &rightAlloc,
                           const std::vector<MatchDoc> &rightDocs,
                           std::vector<std::string> fields) {
    if (leftAlloc == nullptr) {
        AUTIL_LOG(WARN, "left allocator is null");
        return false;
    }
    if (rightAlloc == nullptr) {
        AUTIL_LOG(WARN, "right allocator is null");
        return false;
    }

    if (fields.empty()) {
        const auto &fieldNames = leftAlloc->getAllReferenceNames();
        fields.insert(fields.begin(), fieldNames.begin(), fieldNames.end());
    }
    for (const auto &field : fields) {
        if (false == checkField(leftAlloc, leftDocs, rightAlloc, rightDocs, field)) {
            AUTIL_LOG(WARN, "checkField:[%s] failed", field.c_str());
            return false;
        }
    }
    return true;
}

bool MatchDocHelper::checkDoc(const string &expectVal, MatchDocAllocatorPtr &allocator, MatchDoc doc) {
    if (!allocator) {
        AUTIL_LOG(ERROR, "allocator is null");
        return false;
    }
    vector<ReferenceBase *> refVec;
    vector<string> names;
    vector<string> nameAndVals = StringUtil::split(expectVal, ",", false);
    for (const auto &val : nameAndVals) {
        if (val.empty()) {
            continue;
        }
        vector<string> nameAndVal = StringUtil::split(val, "=", false);
        if (nameAndVal.size() != 2) {
            AUTIL_LOG(ERROR, "invalid nameAndVal:[%s]", val.c_str());
            return false;
        }
        if (nameAndVal[0].empty()) {
            AUTIL_LOG(ERROR, "name:[%s] is empty", nameAndVal[0].c_str());
            return false;
        }
        ReferenceBase *ref = allocator->findReferenceWithoutType(nameAndVal[0]);
        if (!ref) {
            AUTIL_LOG(ERROR, "find ref:[%s] failed", nameAndVal[0].c_str());
            return false;
        }
        refVec.push_back(ref);
        names.push_back(nameAndVal[0]);
    }
    string actualVal = printDoc(refVec, names, doc);
    if (actualVal != expectVal) {
        AUTIL_LOG(ERROR, "actualVals:[%s] != expectVal:[%s]", actualVal.c_str(), expectVal.c_str());
        return false;
    }
    return true;
}

template <typename T>
string MatchDocHelper::refValueToString(const string &name, ReferenceBase *ref, MatchDoc doc) {
    auto *typedRef = dynamic_cast<matchdoc::Reference<T> *>(ref);
    if (nullptr == typedRef) {
        AUTIL_LOG(WARN, "ref[%s] cast to typedRef failed", name.c_str());
        return "";
    }
    const T &value = typedRef->get(doc);
    return StringUtil::toString<T>(value);
}

string MatchDocHelper::printDoc(const vector<ReferenceBase *> &refs, const vector<string> &names, MatchDoc doc) {
    string docContent;
    for (size_t refCount = 0; refCount < refs.size(); refCount++) {
        ReferenceBase *pFieldRef = refs[refCount];
        const string &name = refCount < names.size() ? names[refCount] : pFieldRef->getName();
        matchdoc::ValueType valueType = pFieldRef->getValueType();
        string fieldValue;
        switch (valueType.getBuiltinType()) {
#define CASE_CLAUSE(bt)                                                                                                \
    case bt: {                                                                                                         \
        typedef matchdoc::BuiltinType2CppType<bt>::CppType TYPE;                                                       \
        if (!valueType.isMultiValue()) {                                                                               \
            fieldValue = MatchDocHelper::refValueToString<TYPE>(name, pFieldRef, doc);                                 \
        } else {                                                                                                       \
            if (!valueType.isStdType()) {                                                                              \
                auto *typedRef = static_cast<matchdoc::Reference<autil::MultiValueType<TYPE>> *>(pFieldRef);           \
                autil::MultiValueType<TYPE> multiValue = typedRef->get(doc);                                           \
                vector<TYPE> valueVec;                                                                                 \
                for (uint32_t i = 0; i < multiValue.size(); ++i) {                                                     \
                    const TYPE value = multiValue[i];                                                                  \
                    valueVec.push_back(value);                                                                         \
                }                                                                                                      \
                fieldValue = StringUtil::toString(valueVec);                                                           \
            } else {                                                                                                   \
                fieldValue = MatchDocHelper::refValueToString<vector<TYPE>>(name, pFieldRef, doc);                     \
            }                                                                                                          \
        }                                                                                                              \
        break;                                                                                                         \
    }

            NUMBER_BUILTIN_TYPE_MACRO_HELPER(CASE_CLAUSE);
        case matchdoc::bt_string: {
            if (!valueType.isMultiValue()) {
                if (!valueType.isStdType()) {
                    auto *pTypedRef = static_cast<matchdoc::Reference<autil::MultiChar> *>(pFieldRef);
                    autil::MultiChar value = pTypedRef->get(doc);
                    fieldValue.assign(value.data(), value.size());
                } else {
                    fieldValue = MatchDocHelper::refValueToString<string>(name, pFieldRef, doc);
                }
            } else {
                if (!valueType.isStdType()) {
                    auto *typedRef = static_cast<matchdoc::Reference<autil::MultiString> *>(pFieldRef);
                    autil::MultiString multiValue = typedRef->get(doc);
                    vector<string> valueVec;
                    for (uint32_t i = 0; i < multiValue.size(); ++i) {
                        autil::MultiChar value = multiValue[i];
                        valueVec.emplace_back(value.data(), value.size());
                    }
                    fieldValue = StringUtil::toString(valueVec);
                } else {
                    fieldValue = MatchDocHelper::refValueToString<vector<string>>(name, pFieldRef, doc);
                }
            }
            break;
        }
        default: {
            AUTIL_LOG(WARN, "not support type:[%d]", valueType.getBuiltinType());
            break;
        }
        }
#undef CASE_CLAUSE
        docContent += name + "=" + fieldValue;
        if (refCount < refs.size() - 1) {
            docContent += ",";
        }
    }
    return docContent;
}

vector<string> MatchDocHelper::printAsDocs(const MatchDocAllocatorPtr &allocator,
                                           const std::vector<MatchDoc> &docs,
                                           std::vector<std::string> fields) {
    if (allocator == nullptr) {
        AUTIL_LOG(INFO, "allocator is null");
        return {};
    }
    if (docs.empty()) {
        AUTIL_LOG(INFO, "docs is empty");
        return {};
    }
    if (fields.empty()) {
        const auto &fieldNames = allocator->getAllReferenceNames();
        fields.insert(fields.begin(), fieldNames.begin(), fieldNames.end());
    }
    ReferenceVector refs;
    vector<string> names;
    for (auto &field : fields) {
        auto ref = allocator->findReferenceWithoutType(field);
        if (!ref) {
            AUTIL_LOG(INFO, "can't find ref:[%s]", field.c_str());
            continue;
        }
        refs.push_back(ref);
        names.push_back(field);
    }
    vector<string> result;
    result.reserve(docs.size());
    for (auto doc : docs) {
        result.push_back(printDoc(refs, names, doc));
    }
    return result;
}

string MatchDocHelper::print(const MatchDocAllocatorPtr &allocator,
                             const std::vector<MatchDoc> &docs,
                             std::vector<std::string> fields) {
    vector<string> docStrs = printAsDocs(allocator, docs, fields);
    string result;
    for (auto &docStr : docStrs) {
        result.append(docStr);
        result.append("\n");
    }
    return result;
}

} // namespace matchdoc

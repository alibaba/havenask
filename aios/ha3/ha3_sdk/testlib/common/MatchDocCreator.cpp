#include <ha3_sdk/testlib/common/MatchDocCreator.h>
#include <autil/StringUtil.h>
#include <autil/MultiValueCreator.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <ha3/common/CommonDef.h>

using namespace std;
using namespace autil;
using namespace suez::turing;
BEGIN_HA3_NAMESPACE(common);
HA3_LOG_SETUP(common, MatchDocCreator);

struct TestMatchDocValue {
    string refName;
    VariableType type;
    bool isMultiValue;
    vector<string> values;
};

VariableType parseVariableType(const string &variableTypeStr,
                               bool &isMultiValue)
{
    static const string MULTI_PREFIX = "multi_";
    string singleTypeStr;
    if (variableTypeStr.size() > MULTI_PREFIX.size()
        && variableTypeStr.substr(0, MULTI_PREFIX.size()) == MULTI_PREFIX)
    {
        singleTypeStr = variableTypeStr.substr(MULTI_PREFIX.size());
        isMultiValue = true;
    } else {
        singleTypeStr = variableTypeStr;
        isMultiValue = false;
    }
    if (singleTypeStr == "int8") {
        return vt_int8;
    }
    if (singleTypeStr == "int16") {
        return vt_int16;
    }
    if (singleTypeStr == "int32") {
        return vt_int32;
    }
    if (singleTypeStr == "int64") {
        return vt_int64;
    }
    if (singleTypeStr == "uint8") {
        return vt_uint8;
    }
    if (singleTypeStr == "uint16") {
        return vt_uint16;
    }
    if (singleTypeStr == "uint32") {
        return vt_uint32;
    }
    if (singleTypeStr == "uint64") {
        return vt_uint64;
    }
    if (singleTypeStr == "float") {
        return vt_float;
    }
    if (singleTypeStr == "double") {
        return vt_double;
    }
    if (singleTypeStr == "string") {
        return vt_string;
    }
    if (singleTypeStr == "bool") {
        return vt_bool;
    }
    if (singleTypeStr == "hash128") {
        return vt_hash_128;
    }
    assert(false);
    return vt_unknown;
}

void processInnerValue(TestMatchDocValue &oneRef,
                       vector<matchdoc::MatchDoc> &matchDocs,
                       common::Ha3MatchDocAllocator *mdAllocator)
{
    if (oneRef.refName == "docid") {
        for (size_t i = 0; i < matchDocs.size(); ++i) {
            matchDocs[i].setDocId(StringUtil::fromString<docid_t>(oneRef.values[i]));
        }
    }
    if (oneRef.refName == "hashid") {
        auto ref = mdAllocator->findReference<hashid_t>(common::HASH_ID_REF);
        for (size_t i = 0; i < matchDocs.size(); ++i) {
            ref->set(matchDocs[i], StringUtil::fromString<hashid_t>(oneRef.values[i]));
        }
    }
    if (oneRef.refName == "clusterid") {
        auto ref = mdAllocator->findReference<clusterid_t>(CLUSTER_ID_REF);
        for (size_t i = 0; i < matchDocs.size(); ++i) {
            ref->set(matchDocs[i], StringUtil::fromString<clusterid_t>(oneRef.values[i]));
        }
    }
}

vector<TestMatchDocValue> parseMatchDocStr(const string &matchDocStr) {
    vector<TestMatchDocValue> ret;
    StringTokenizer multiRefs(matchDocStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY
                              | StringTokenizer::TOKEN_TRIM);
    ret.resize(multiRefs.getNumTokens());
    set<string> innerRefName;
    innerRefName.insert("pkattr");
    innerRefName.insert("rawpk");
    innerRefName.insert("indexversion");
    innerRefName.insert("fullversion");
    innerRefName.insert("ip");
    innerRefName.insert("globalid");
    innerRefName.insert("score");
    for (size_t i = 0; i < multiRefs.getNumTokens(); ++i) {
        StringTokenizer oneRef(multiRefs[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY
                               | StringTokenizer::TOKEN_TRIM);
        assert(oneRef.getNumTokens() == 3);
        ret[i].refName = oneRef[0];
        if (innerRefName.find(ret[i].refName) != innerRefName.end()) {
            ret[i].refName = suez::turing::BUILD_IN_REFERENCE_PREFIX + ret[i].refName;
        }
        ret[i].type = parseVariableType(oneRef[1], ret[i].isMultiValue);
        StringUtil::fromString(oneRef[2], ret[i].values, ",");
    }
    return ret;
}

MatchDocCreator::MatchDocCreator() {
    _pool = new autil::mem_pool::Pool;
    _mdAllocator = new Ha3MatchDocAllocator(_pool);
}

MatchDocCreator::~MatchDocCreator() {
    DELETE_AND_SET_NULL(_mdAllocator);
    DELETE_AND_SET_NULL(_pool);
}

void MatchDocCreator::createMatchDocs(const string &matchDocStr,
                                      vector<matchdoc::MatchDoc> &matchDocs)
{
    createMatchDocs(matchDocStr, matchDocs, _mdAllocator, _pool);
}

void MatchDocCreator::destroyMatchDocs(const vector<matchdoc::MatchDoc> &matchDocs) {
    deallocateMatchDocs(matchDocs, _mdAllocator);
}

// key:type:values
// example:
// "PrimaryKey:primarykey_t:1,2,3,4,5;ClusterId:uint32_t:3,4,5,6,7"
void MatchDocCreator::createMatchDocs(const string &matchDocStr,
                                      vector<matchdoc::MatchDoc> &matchDocs,
                                      common::Ha3MatchDocAllocator *mdAllocator,
                                      autil::mem_pool::Pool *pool)
{
    vector<TestMatchDocValue> refs = parseMatchDocStr(matchDocStr);

    set<string> innerValue;
    innerValue.insert("docid");
    innerValue.insert("hashid");
    innerValue.insert("clusterid");

#define DECLARE_VARIABLE_HELPER(vt)                                     \
    case vt:                                                            \
    {                                                                   \
        if (!refs[i].isMultiValue) {                                    \
            typedef VariableTypeTraits<vt, false>::AttrExprType T;      \
            mdAllocator->declare<T>(refs[i].refName);                   \
        } else {                                                        \
            typedef VariableTypeTraits<vt, true>::AttrExprType T;       \
            mdAllocator->declare<T>(refs[i].refName);                   \
        }                                                               \
    }                                                                   \
    break

    TestMatchDocValue *docIdValue = NULL;
    for (size_t i = 0; i < refs.size(); ++i) {
        if (refs[i].refName == "docid") {
            docIdValue = &refs[i];
        }
        if (refs[i].refName == "hashid") {
            mdAllocator->declare<hashid_t>(common::HASH_ID_REF, SL_QRS);
            continue;
        } else if (refs[i].refName == "clusterid") {
            mdAllocator->declare<clusterid_t>(CLUSTER_ID_REF,
                    CLUSTER_ID_REF_GROUP);
            continue;
        }
        if (innerValue.find(refs[i].refName) != innerValue.end()) {
            continue;
        }
        switch (refs[i].type) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(DECLARE_VARIABLE_HELPER);
            DECLARE_VARIABLE_HELPER(vt_string);
        case vt_hash_128:
        {
            mdAllocator->declare<primarykey_t>(refs[i].refName);
            break;
        }
        default:
            break;
        }
    }

    for (size_t i = 0; i < docIdValue->values.size(); ++i) {
        matchDocs.push_back(mdAllocator->allocate(
                        StringUtil::fromString<docid_t>(docIdValue->values[i])));
    }

#define SET_VARIABLE_VALUE_HELPER(vt)                                   \
    case vt:                                                            \
    {                                                                   \
        typedef VariableTypeTraits<vt, false>::AttrExprType T;          \
        if (!refs[i].isMultiValue) {                                    \
            auto typedRef = mdAllocator->findReference<T>( \
                    refs[i].refName);                                   \
            for (size_t j = 0; j < refs[i].values.size(); ++j) {        \
                T value = StringUtil::fromString<T>(refs[i].values[j]); \
                typedRef->set(matchDocs[j], value);  \
            }                                                           \
        } else {                                                        \
            typedef MultiValueType<T> MultiType;                        \
            auto typedRef = mdAllocator->findReference<MultiType>( \
                    refs[i].refName);                                   \
            for (size_t j = 0; j < refs[i].values.size(); ++j) {        \
                StringTokenizer multiValue(refs[i].values[j], "#", StringTokenizer::TOKEN_IGNORE_EMPTY \
                        | StringTokenizer::TOKEN_TRIM);                 \
                T *buffer = (T*)pool->allocate(sizeof(T) * multiValue.getNumTokens()); \
                for (size_t k = 0; k < multiValue.getNumTokens(); ++k) { \
                    buffer[k] = StringUtil::fromString<T>(multiValue[k]); \
                }                                                       \
                MultiType value;                                        \
                value.init(MultiValueCreator::createMultiValueBuffer(buffer, multiValue.getNumTokens(), pool)); \
                typedRef->set(matchDocs[j], value);  \
            }                                                           \
        }                                                               \
    }                                                                   \
    break

    for (size_t i = 0; i < refs.size(); ++i) {
        if (innerValue.find(refs[i].refName) != innerValue.end()) {
            processInnerValue(refs[i], matchDocs, mdAllocator);
            continue;
        }
        switch (refs[i].type) {
            NUMERIC_VARIABLE_TYPE_MACRO_HELPER_WITH_BOOL(SET_VARIABLE_VALUE_HELPER);
        case vt_hash_128:
        {
            typedef primarykey_t T;
            auto typedRef = mdAllocator->findReference<T>(
                    refs[i].refName);
            for (size_t j = 0; j < refs[i].values.size(); ++j) {
                T value = StringUtil::fromString<T>(refs[i].values[j]);
                typedRef->set(matchDocs[j], value);
            }
            break;
        }
        case vt_string:
        {
            // TODO: support multi string
            assert(!refs[i].isMultiValue);
            auto typedRef = mdAllocator->findReference<MultiChar>(
                    refs[i].refName);
            for (size_t j = 0; j < refs[i].values.size(); ++j) {
                size_t sz = refs[i].values[j].size();
                void *buf = pool->allocate(sz);
                memcpy(buf, refs[i].values[j].data(), sz);
                MultiChar mc;
                mc.init(MultiValueCreator::createMultiValueBuffer<char>((char*)buf, sz, pool));
                typedRef->set(matchDocs[j], mc);
            }
            break;
        }
        default:
            break;
        }
    }

}

void MatchDocCreator::allocateMatchDocs(const string &docIdStr,
                                        vector<matchdoc::MatchDoc> &matchDocs,
                                        common::Ha3MatchDocAllocator *mdAllocator)
{
    vector<docid_t> docIds;
    StringUtil::fromString(docIdStr, docIds, ",");
    for (size_t i = 0; i < docIds.size(); i++) {
        auto matchDoc = mdAllocator->allocate(docIds[i]);
        matchDocs.push_back(matchDoc);
    }
}

void MatchDocCreator::deallocateMatchDocs(const vector<matchdoc::MatchDoc> &matchDocs,
        common::Ha3MatchDocAllocator *mdAllocator)
{
    for (size_t i = 0; i < matchDocs.size(); i++) {
        mdAllocator->deallocate(matchDocs[i]);
    }
}

void MatchDocCreator::allocateMatchDocWithSubDoc(
        const string &docIdStr,
        vector<matchdoc::MatchDoc> &matchDocs,
        common::Ha3MatchDocAllocator *allocator)
{
    vector<int> docIds;
    vector<int> subDocIdNums;
    vector<int> subDocIds;
    createDocVector(docIdStr, docIds, subDocIdNums, subDocIds);

    SimpleDocIdIterator docIdIterator(docIds, subDocIdNums, subDocIds);
    while (docIdIterator.hasNext()) {
        matchDocs.push_back(allocator->allocateWithSubDoc(&docIdIterator));
        docIdIterator.next();
    }
}

// docId layout: "docid1:subdocid1, subdocid2; docid2:subdocid1, subdocid2;"
SimpleDocIdIterator MatchDocCreator::createDocIterator(const string &docStr)
{
    vector<int> docIds;
    vector<int> subDocIdNums;
    vector<int> subDocIds;
    createDocVector(docStr, docIds, subDocIdNums, subDocIds);

    return SimpleDocIdIterator(docIds, subDocIdNums, subDocIds);
}

void MatchDocCreator::createDocVector(const string &docStr,
                                      vector<int> &docIds, vector<int> &subDocIdNums, vector<int> &subDocIds)
{
    StringTokenizer st(docStr, ";", StringTokenizer::TOKEN_IGNORE_EMPTY |
                       StringTokenizer::TOKEN_TRIM);
    for (size_t i = 0; i < st.getNumTokens(); i++) {
        StringTokenizer st1(st[i], ":", StringTokenizer::TOKEN_IGNORE_EMPTY |
                            StringTokenizer::TOKEN_TRIM);
        docIds.push_back(StringUtil::fromString<int>(st1[0]));
        if (2 == st1.getNumTokens()) {
            StringTokenizer st2(st1[1], ",", StringTokenizer::TOKEN_IGNORE_EMPTY |
                    StringTokenizer::TOKEN_TRIM);
            subDocIdNums.push_back(st2.getNumTokens());
            for (size_t j = 0; j < st2.getNumTokens(); j++) {
                subDocIds.push_back(StringUtil::fromString<int>(st2[j]));
            }
        } else {
            subDocIdNums.push_back(0);
        }
    }
}

END_HA3_NAMESPACE(common);

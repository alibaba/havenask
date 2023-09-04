#include "matchdoc/toolkit/MatchDocTestHelper.h"

#include <assert.h>
#include <ext/alloc_traits.h>
#include <map>
#include <memory>
#include <stddef.h>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/StringUtil.h"
#include "autil/mem_pool/Pool.h"
#include "matchdoc/Reference.h"
#include "matchdoc/SubDocAccessor.h"

using namespace std;
using namespace autil;
using namespace autil::mem_pool;

namespace matchdoc {

// despStr: "group_a:int_a,sub_float_b" if not specify groupName, use default
MatchDocAllocatorPtr MatchDocTestHelper::createAllocator(const string &despStr,
                                                         const std::shared_ptr<autil::mem_pool::Pool> &pool,
                                                         const MountInfoPtr &mountInfo) {
    MatchDocAllocatorPtr matchDocAllocator(new MatchDocAllocator(pool, false, mountInfo));

    vector<vector<string>> fieldInfos;
    StringUtil::fromString(despStr, fieldInfos, ":", ",");
    for (size_t i = 0; i < fieldInfos.size(); ++i) {
        assert(fieldInfos[i].size() <= 2);
        const string &groupName = fieldInfos[i].size() == 1 ? DEFAULT_GROUP : fieldInfos[i][0];
        const string &fieldName = fieldInfos[i].size() == 1 ? fieldInfos[i][0] : fieldInfos[i][1];

        bool isSub = false;
        string type = fieldName.substr(0, fieldName.find('_'));
        if (type == "sub") {
            isSub = true;
            string remainName = fieldName.substr(fieldName.find('_') + 1);
            type = remainName.substr(0, remainName.find('_'));
        }

        if (isSub) {
            if (type == "int") {
                matchDocAllocator->declareSub<int>(fieldName, groupName);
            } else if (type == "string") {
                matchDocAllocator->declareSub<string>(fieldName, groupName);
            } else if (type == "float") {
                matchDocAllocator->declareSub<float>(fieldName, groupName);
            } else {
                assert(false);
            }
        } else {
            if (type == "int") {
                matchDocAllocator->declare<int>(fieldName, groupName);
            } else if (type == "string") {
                matchDocAllocator->declare<string>(fieldName, groupName);
            } else if (type == "float") {
                matchDocAllocator->declare<float>(fieldName, groupName);
            } else {
                assert(false);
            }
        }
    }
    matchDocAllocator->extend();
    if (matchDocAllocator->hasSubDocAllocator()) {
        matchDocAllocator->extendSub();
    }
    return matchDocAllocator;
}

// valueStr: "0,3^4^5#string_c:hello,string_d:kitty,sub_int_a:1^3^5,..."
MatchDoc MatchDocTestHelper::createMatchDoc(const MatchDocAllocatorPtr &allocator, const std::string &valueStr) {
    vector<string> valueInfo;
    StringUtil::fromString(valueStr, valueInfo, "#");
    assert(valueInfo.size() <= 2);

    int32_t mainDocid = -1;
    vector<int32_t> subDocids;

    string docValueStr = valueStr;
    if (valueInfo.size() == 2) {
        vector<string> docStrVec;
        StringUtil::fromString(valueInfo[0], docStrVec, ",");
        assert(docStrVec.size() <= 2);
        mainDocid = StringUtil::fromString<int32_t>(docStrVec[0]);
        if (docStrVec.size() == 2) {
            StringUtil::fromString(docStrVec[1], subDocids, "^");
        }
        docValueStr = valueInfo[1];
    }

    MatchDoc matchDoc = allocator->allocate(mainDocid);
    vector<MatchDoc> subDocs;

    vector<vector<string>> fieldInfos;
    StringUtil::fromString(docValueStr, fieldInfos, ":", ",");
    for (size_t i = 0; i < fieldInfos.size(); ++i) {
        assert(fieldInfos[i].size() == 2);
        const string &fieldName = fieldInfos[i][0];
        const string &fieldValue = fieldInfos[i][1];

        bool isSub = false;
        string type = fieldName.substr(0, fieldName.find('_'));
        if (type == "sub") {
            isSub = true;
            string remainName = fieldName.substr(fieldName.find('_') + 1);
            type = remainName.substr(0, remainName.find('_'));
        }

#define SET_VALUE(T)                                                                                                   \
    Reference<T> *ref = allocator->findReference<T>(fieldName);                                                        \
    assert(ref);                                                                                                       \
    assert(ref->isSubDocReference() == isSub);                                                                         \
    if (!isSub) {                                                                                                      \
        ref->set(matchDoc, StringUtil::fromString<T>(fieldValue));                                                     \
    } else {                                                                                                           \
        vector<T> values;                                                                                              \
        StringUtil::fromString(fieldValue, values, "^");                                                               \
        if (subDocs.empty()) {                                                                                         \
            subDocids.resize(values.size(), -1);                                                                       \
            for (size_t i = 0; i < values.size(); ++i) {                                                               \
                subDocs.push_back(allocator->allocateSub(matchDoc, subDocids[i]));                                     \
            }                                                                                                          \
        }                                                                                                              \
        Reference<MatchDoc> *currentRef = ref->getCurrentRef();                                                        \
        assert(currentRef);                                                                                            \
        assert(subDocs.size() == values.size());                                                                       \
        for (size_t i = 0; i < values.size(); ++i) {                                                                   \
            currentRef->set(matchDoc, subDocs[i]);                                                                     \
            ref->set(matchDoc, values[i]);                                                                             \
        }                                                                                                              \
    }

        if (type == "int") {
            SET_VALUE(int);
        } else if (type == "string") {
            SET_VALUE(string);
        } else {
            assert(false);
        }
#undef SET_VALUE
    }
    return matchDoc;
}

template <typename T>
class SubValueChecker {
public:
    // expectValueStr : 1^2^3
    SubValueChecker(Reference<T> *subField, const std::string &expectValueStr) : _subFieldRef(subField) {
        StringUtil::fromString(expectValueStr, _expectValues, "^");
    }

public:
    void operator()(MatchDoc doc) { _values.push_back(_subFieldRef->get(doc)); }

    void resetExpectValue(T value) {
        for (size_t i = 0; i < _expectValues.size(); i++) {
            _expectValues[i] = value;
        }
    }

    bool check() { return _expectValues == _values; }

private:
    Reference<T> *_subFieldRef;
    vector<T> _expectValues;
    vector<T> _values;
};

// expectedValue: "int_a:4,int_b:6,string_c:hello,string_d:kitty"
bool MatchDocTestHelper::checkDocValue(const MatchDocAllocatorPtr &allocator,
                                       const MatchDoc &matchDoc,
                                       const std::string &expectedValue) {
    vector<vector<string>> fieldInfos;
    StringUtil::fromString(expectedValue, fieldInfos, ":", ",");
    for (size_t i = 0; i < fieldInfos.size(); ++i) {
        assert(fieldInfos[i].size() == 2);
        const string &fieldName = fieldInfos[i][0];
        const string &valueStr = fieldInfos[i][1];

        bool isSub = false;
        string type = fieldName.substr(0, fieldName.find('_'));
        if (type == "sub") {
            isSub = true;
            string remainName = fieldName.substr(fieldName.find('_') + 1);
            type = remainName.substr(0, remainName.find('_'));
        }

#define CHECK_VALUE(T)                                                                                                 \
    Reference<T> *ref = allocator->findReference<T>(fieldName);                                                        \
    assert(ref);                                                                                                       \
    assert(ref->isSubDocReference() == isSub);                                                                         \
    if (!isSub) {                                                                                                      \
        if (StringUtil::fromString<T>(valueStr) != ref->get(matchDoc)) {                                               \
            return false;                                                                                              \
        }                                                                                                              \
    } else {                                                                                                           \
        SubValueChecker<T> checker(ref, valueStr);                                                                     \
        allocator->getSubDocAccessor()->foreach (matchDoc, checker);                                                   \
        if (!checker.check()) {                                                                                        \
            return false;                                                                                              \
        }                                                                                                              \
    }

        if (type == "int") {
            CHECK_VALUE(int);
        } else if (type == "string") {
            CHECK_VALUE(string);
        } else if (type == "float") {
            CHECK_VALUE(float);
        } else {
            assert(false);
        }

#undef CHECK_VALUE
    }
    return true;
}

bool MatchDocTestHelper::checkDeserializedDocValue(const MatchDocAllocatorPtr &allocator,
                                                   const MatchDoc &matchDoc,
                                                   uint8_t serializeLevel,
                                                   const string &expectedValue) {
    vector<vector<string>> fieldInfos;
    StringUtil::fromString(expectedValue, fieldInfos, ":", ",");
    for (size_t i = 0; i < fieldInfos.size(); ++i) {
        assert(fieldInfos[i].size() == 2);
        const string &fieldName = fieldInfos[i][0];
        string valueStr = fieldInfos[i][1];
        ReferenceBase *refBase = allocator->findReferenceWithoutType(fieldName);
        if (!refBase) {
            continue;
        }

        bool isSub = false;
        string type = fieldName.substr(0, fieldName.find('_'));
        if (type == "sub") {
            isSub = true;
            string remainName = fieldName.substr(fieldName.find('_') + 1);
            type = remainName.substr(0, remainName.find('_'));
        }

#define CHECK_VALUE(T)                                                                                                 \
    Reference<T> *ref = allocator->findReference<T>(fieldName);                                                        \
    assert(ref);                                                                                                       \
    assert(ref->isSubDocReference() == isSub);                                                                         \
    if (!isSub) {                                                                                                      \
        if (StringUtil::fromString<T>(valueStr) != ref->get(matchDoc)) {                                               \
            return false;                                                                                              \
        }                                                                                                              \
    } else {                                                                                                           \
        SubValueChecker<T> checker(ref, valueStr);                                                                     \
        allocator->getSubDocAccessor()->foreach (matchDoc, checker);                                                   \
        return checker.check();                                                                                        \
    }

        if (type == "int") {
            CHECK_VALUE(int);
        } else if (type == "string") {
            CHECK_VALUE(string);
        } else if (type == "float") {
            CHECK_VALUE(float);
        } else {
            assert(false);
        }

#undef CHECK_VALUE
    }
    return true;
}

// despStr: "int:10,float:22.2,int:50". only support int and float for now
char *MatchDocTestHelper::createMountData(const string &despStr, Pool *pool) {
    vector<vector<string>> fieldInfo;
    StringUtil::fromString(despStr, fieldInfo, ":", ",");
    size_t maxBufSize = 1024;
    char *buf = fieldInfo.size() ? (char *)pool->allocate(maxBufSize) : NULL;
    char *cur = buf;

    for (size_t i = 0; i < fieldInfo.size(); ++i) {
        assert(fieldInfo[i].size() == 2);
        const string &type = fieldInfo[i][0];
        const string &value = fieldInfo[i][1];

        if (type == "int") {
            *(int *)cur = StringUtil::fromString<int>(value);
            cur += sizeof(int);
        } else if (type == "float") {
            *(float *)cur = StringUtil::fromString<float>(value);
            cur += sizeof(float);
        } else {
            assert(false);
        }
    }
    return buf;
}

// despStr: "1:int_a,float_b,int_c;2:int_c"
MountInfoPtr MatchDocTestHelper::createMountInfo(const string &despStr) {
    vector<vector<string>> fieldInfos;
    MountInfoPtr mountInfo(new MountInfo);
    StringUtil::fromString(despStr, fieldInfos, ":", ";");
    for (size_t i = 0; i < fieldInfos.size(); ++i) {
        assert(fieldInfos[i].size() == 2);
        uint32_t mountId = StringUtil::fromString<uint32_t>(fieldInfos[i][0]);
        vector<string> fields;
        StringUtil::fromString(fieldInfos[i][1], fields, ",");
        uint64_t mountOffset = 0;
        for (size_t j = 0; j < fields.size(); ++j) {
            const string &fieldName = fields[j];
            string type = fieldName.substr(0, fieldName.find('_'));
            mountInfo->insert(fieldName, mountId, mountOffset);
            if (type == "int") {
                mountOffset += sizeof(int);
            } else if (type == "float") {
                mountOffset += sizeof(float);
            } else {
                assert(false);
            }
        }
    }
    return mountInfo;
}

// valueStr: "r1:0,r2:10"
bool MatchDocTestHelper::setSerializeLevel(const MatchDocAllocatorPtr &allocator, const string &levelStr) {
    vector<vector<string>> levelInfos;
    StringUtil::fromString(levelStr, levelInfos, ":", ",");

    for (size_t i = 0; i < levelInfos.size(); ++i) {
        assert(levelInfos[i].size() == 2);
        ReferenceBase *ref = allocator->findReferenceWithoutType(levelInfos[i][0]);
        if (!ref) {
            return false;
        }

        uint8_t sl_level;
        StringUtil::strToUInt8(levelInfos[i][1].c_str(), sl_level);
        ref->setSerializeLevel(sl_level);
    }
    return true;
}

bool MatchDocTestHelper::checkSerializeLevel(const MatchDocAllocatorPtr &allocator,
                                             uint8_t serializeLevel,
                                             const string &expectLevelStr) {
    vector<vector<string>> expectLevelInfos;
    StringUtil::fromString(expectLevelStr, expectLevelInfos, ":", ",");

    for (size_t i = 0; i < expectLevelInfos.size(); ++i) {
        assert(expectLevelInfos[i].size() == 2);

        if (StringUtil::fromString<uint8_t>(expectLevelInfos[i][1]) < serializeLevel) {
            if (allocator->findReferenceWithoutType(expectLevelInfos[i][0])) {
                return false;
            }
            continue;
        }

        ReferenceBase *ref = allocator->findReferenceWithoutType(expectLevelInfos[i][0]);
        if (!ref) {
            return false;
        }

        uint8_t sl_level;
        StringUtil::strToUInt8(expectLevelInfos[i][1].c_str(), sl_level);
        if (sl_level != ref->getSerializeLevel()) {
            return false;
        }
    }
    return true;
}

// valueStr: "alias1:ref1,alias2:ref2"
MatchDocAllocator::ReferenceAliasMapPtr MatchDocTestHelper::createReferenceAliasMap(const string &aliasRelation) {
    vector<vector<string>> aliasInfo;
    StringUtil::fromString(aliasRelation, aliasInfo, ":", ",");

    MatchDocAllocator::ReferenceAliasMapPtr aliasMap(new MatchDocAllocator::ReferenceAliasMap);
    for (size_t i = 0; i < aliasInfo.size(); i++) {
        assert(aliasInfo[i].size() == 2);
        aliasMap->insert(make_pair(aliasInfo[i][0], aliasInfo[i][1]));
    }
    return aliasMap;
}
} // namespace matchdoc

#ifndef ISEARCH_FUNCTIONTESTHELPER_H
#define ISEARCH_FUNCTIONTESTHELPER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/DataProvider.h>
#include <ha3/common/Ha3MatchDocAllocator.h>
#include <suez/turing/expression/function/FunctionInterface.h>
#include <ha3/func_expression/FunctionProvider.h>
#include <ha3_sdk/testlib/search/FakeAttributeExpressionMaker.h>
#include <autil/StringUtil.h>
#include <autil/MultiValueType.h>

BEGIN_HA3_NAMESPACE(func_expression);

class FunctionTestHelper
{
public:
    FunctionTestHelper();
    ~FunctionTestHelper();
private:
    FunctionTestHelper(const FunctionTestHelper &);
    FunctionTestHelper& operator=(const FunctionTestHelper &);
public:
    // format: docid,docid,docid...
    void allocateMatchDocs(const std::string &docIdStr,
                           std::vector<matchdoc::MatchDoc> &matchDocVec);

    void deallocateMatchDocs(const std::vector<matchdoc::MatchDoc> &matchDocs);

    // format: val1,val2,...
    template <typename T>
    bool testFunctionEvaluate(suez::turing::FunctionInterface *fn, const std::string &docIdStr,
                              const std::string &expectedValueStr);

    // format: val1,val2,...
    template <typename T>
    bool testFunctionEvaluate(suez::turing::FunctionInterface *fn,
                              std::vector<matchdoc::MatchDoc> &matchDocs,
                              const std::string &expectedValueStr);

    // format: val11,val12#val21,val22,val23#...
    template <typename T>
    bool testMultiValueFunctionEvaluate(
            suez::turing::FunctionInterface *fn,
            const std::string &docIdStr,
            const std::string &expectedValueStr);

    template <typename T>
    bool testMultiValueFunctionEvaluate(
            suez::turing::FunctionInterface *fn,
            std::vector<matchdoc::MatchDoc> &matchDocs,
            const std::string &expectedValueStr);

    // format: string1,string2,string3
    bool testStringValueFunctionEvaluate(
            suez::turing::FunctionInterface *fn,
            const std::string &docIdStr,
            const std::string &expectedValueStr);

    bool testStringValueFunctionEvaluate(
            suez::turing::FunctionInterface *fn,
            std::vector<matchdoc::MatchDoc> &matchDocs,
            const std::string &expectedValueStr);

    // format: str11,str12#str21,str22#...
    bool testMultiStringValueFunctionEvaluate(
            suez::turing::FunctionInterface *fn,
            const std::string &docIdStr,
            const std::string &expectedValueStr);

    bool testMultiStringValueFunctionEvaluate(
            suez::turing::FunctionInterface *fn,
            std::vector<matchdoc::MatchDoc> &matchDocs,
            const std::string &expectedValueStr);

    // format: val1,val2,...
    template <typename T>
    bool assertVariable(const std::string &variableName,
                        std::vector<matchdoc::MatchDoc> &matchDocs,
                        const std::string &expectedValueStr);
public:
    common::DataProvider* getDataProvider() const {
        return _dataProvider;
    }

    FunctionProvider* getFunctionProvider() const {
        return _funcProvider;
    }
    
    common::Ha3MatchDocAllocator* getMatchDocAllocator() const {
        return _mdAllocator;
    }
    search::FakeAttributeExpressionMaker* getAttrExprMaker() const {
        return _attrExprMaker;
    }
    autil::mem_pool::Pool* getMemPool() const {
        return _pool;
    }
private:
    void init();
    void clear();
private:
    autil::mem_pool::Pool *_pool;
    common::Ha3MatchDocAllocator *_mdAllocator;
    common::DataProvider *_dataProvider;
    FunctionProvider* _funcProvider;
    search::FakeAttributeExpressionMaker *_attrExprMaker;
private:
    HA3_LOG_DECLARE();
};

/////////////////////////////////////////////////////////////////
template <typename T>
bool FunctionTestHelper::testFunctionEvaluate(suez::turing::FunctionInterface *fn,
        const std::string &docIdStr,
        const std::string &expectedValueStr)
{
    if (!fn) {
        return false;
    }
    std::vector<matchdoc::MatchDoc> matchDocs;
    allocateMatchDocs(docIdStr, matchDocs);
    bool allEqual = testFunctionEvaluate<T>(fn, matchDocs, expectedValueStr);
    deallocateMatchDocs(matchDocs);
    return allEqual;
}

template <typename T>
bool FunctionTestHelper::testFunctionEvaluate(suez::turing::FunctionInterface *fn,
        std::vector<matchdoc::MatchDoc> &matchDocs,
        const std::string &expectedValueStr)
{
    suez::turing::FunctionInterfaceTyped<T> *fnt =
        dynamic_cast<suez::turing::FunctionInterfaceTyped<T>* >(fn);
    if (!fnt) {
        return false;
    }
    std::vector<T> expectedValues;
    autil::StringUtil::fromString<T>(expectedValueStr, expectedValues, ",");
    if (matchDocs.size() != expectedValues.size()) {
        return false;
    }
    bool allEqual = true;
    for (size_t i = 0; i < matchDocs.size(); i++) {
        T val = fnt->evaluate(matchDocs[i]);
        if (expectedValues[i] != val) {
            allEqual = false;
            break;
        }
    }
    return allEqual;
}

template <typename T>
inline bool FunctionTestHelper::testMultiValueFunctionEvaluate(
        suez::turing::FunctionInterface *fn,
        const std::string &docIdStr,
        const std::string &expectedValueStr)
{
    std::vector<matchdoc::MatchDoc> matchDocs;
    allocateMatchDocs(docIdStr, matchDocs);
    bool allEqual = testMultiValueFunctionEvaluate<T>(fn, matchDocs, expectedValueStr);
    deallocateMatchDocs(matchDocs);
    return allEqual;
}

template <typename T>
inline bool FunctionTestHelper::testMultiValueFunctionEvaluate(
        suez::turing::FunctionInterface *fn,
        std::vector<matchdoc::MatchDoc> &matchDocs,
        const std::string &expectedValueStr)
{
    typedef autil::MultiValueType<T> MultiValueType;
    suez::turing::FunctionInterfaceTyped<MultiValueType> *fnt =
        dynamic_cast<suez::turing::FunctionInterfaceTyped<MultiValueType>* >(fn);
    if (!fnt) {
        return false;
    }
    std::vector<std::vector<T> > expectedValues;
    autil::StringUtil::fromString<T>(expectedValueStr, expectedValues, ",", "#");

    if (matchDocs.size() != expectedValues.size()) {
        return false;
    }

    bool allEqual = true;
    for (size_t i = 0; i < matchDocs.size(); i++) {
        MultiValueType value = fnt->evaluate(matchDocs[i]);
        if (value.size() != expectedValues[i].size()) {
            allEqual = false;
            break;
        }
        for (size_t j = 0; j < value.size(); j++) {
            if (value[j] != expectedValues[i][j]) {
                allEqual = false;
                break;
            }
        }
        if (!allEqual) {
            break;
        }
    }
    return allEqual;
}

inline bool FunctionTestHelper::testStringValueFunctionEvaluate(
        suez::turing::FunctionInterface *fn,
        const std::string &docIdStr,
        const std::string &expectedValueStr)
{
    std::vector<matchdoc::MatchDoc> matchDocs;
    allocateMatchDocs(docIdStr, matchDocs);
    bool allEqual = testStringValueFunctionEvaluate(fn,
            matchDocs, expectedValueStr);
    deallocateMatchDocs(matchDocs);
    return allEqual;
}

inline bool FunctionTestHelper::testStringValueFunctionEvaluate(
        suez::turing::FunctionInterface *fn,
        std::vector<matchdoc::MatchDoc> &matchDocs,
        const std::string &expectedValueStr)
{
    typedef autil::MultiValueType<char> MultiChar;
    suez::turing::FunctionInterfaceTyped<MultiChar> *fnt =
        dynamic_cast<suez::turing::FunctionInterfaceTyped<MultiChar>* >(fn);
    if (!fnt) {
        return false;
    }
    std::vector<std::string> expectedValues;
    autil::StringUtil::fromString<std::string>(expectedValueStr,
            expectedValues, ",");

    if (matchDocs.size() != expectedValues.size()) {
        return false;
    }

    bool allEqual = true;
    for (size_t i = 0; i < matchDocs.size(); i++) {
        MultiChar value = fnt->evaluate(matchDocs[i]);
        if (value.size() != expectedValues[i].size()) {
            allEqual = false;
            break;
        }
        for (size_t j = 0; j < value.size(); j++) {
            if (value[j] != expectedValues[i][j]) {
                allEqual = false;
                break;
            }
        }
        if (!allEqual) {
            break;
        }
    }
    return allEqual;
}

inline bool FunctionTestHelper::testMultiStringValueFunctionEvaluate(
        suez::turing::FunctionInterface *fn,
        const std::string &docIdStr,
        const std::string &expectedValueStr)
{
    std::vector<matchdoc::MatchDoc> matchDocs;
    allocateMatchDocs(docIdStr, matchDocs);
    bool allEqual = testMultiStringValueFunctionEvaluate(fn,
            matchDocs, expectedValueStr);
    deallocateMatchDocs(matchDocs);
    return allEqual;
}

inline bool FunctionTestHelper::testMultiStringValueFunctionEvaluate(
        suez::turing::FunctionInterface *fn,
        std::vector<matchdoc::MatchDoc> &matchDocs,
        const std::string &expectedValueStr)
{
    typedef autil::MultiValueType<char> MultiChar;
    typedef autil::MultiValueType<MultiChar> MultiString;
    suez::turing::FunctionInterfaceTyped<MultiString> *fnt =
        dynamic_cast<suez::turing::FunctionInterfaceTyped<MultiString>* >(fn);
    if (!fnt) {
        return false;
    }
    std::vector<std::vector<std::string> > expectedValues;
    autil::StringUtil::fromString<std::string>(expectedValueStr,
            expectedValues, ",", "#");

    if (matchDocs.size() != expectedValues.size()) {
        return false;
    }

    bool allEqual = true;
    for (size_t i = 0; i < matchDocs.size(); i++) {
        MultiString value = fnt->evaluate(matchDocs[i]);
        if (value.size() != expectedValues[i].size()) {
            allEqual = false;
            break;
        }
        for (size_t j = 0; j < value.size(); j++) {
            std::string actualValue(value[j].data(), value[j].size());
            if (actualValue != expectedValues[i][j]) {
                allEqual = false;
                break;
            }
        }
        if (!allEqual) {
            break;
        }
    }
    return allEqual;
}

template <typename T>
bool FunctionTestHelper::assertVariable(const std::string &variableName,
                                        std::vector<matchdoc::MatchDoc> &matchDocs,
                                        const std::string &expectedValueStr)
{
    matchdoc::Reference<T> *ref = nullptr;
    // todo
        // _dataProvider->findVariableReference<T>(variableName);
    if (!ref) {
        return false;
    }
    std::vector<T> expectedVals;
    autil::StringUtil::fromString<T>(expectedValueStr, expectedVals, ",");
    if (matchDocs.size() != expectedVals.size()) {
        return false;
    }
    bool allEqual = true;
    for (size_t i = 0; i < matchDocs.size(); i++) {
        T val;
        val = ref->get(matchDocs[i]);
        if (val != expectedVals[i]) {
            allEqual = false;
            break;
        }
    }
    return allEqual;
}

HA3_TYPEDEF_PTR(FunctionTestHelper);

END_HA3_NAMESPACE(func_expression);

#endif //ISEARCH_FUNCTIONTESTHELPER_H

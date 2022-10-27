#ifndef ISEARCH_PROVIDERBASE_H
#define ISEARCH_PROVIDERBASE_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/AggResultReader.h>
#include <ha3/common/QueryTerminator.h>
#include <ha3/search/SearchPluginResource.h>
#include <suez/turing/expression/framework/ExpressionEvaluator.h>
#include <ha3/rank/MatchData.h>
#include <ha3/rank/SimpleMatchData.h>
#include <ha3/rank/MatchValues.h>
#include <indexlib/misc/pool_util.h>

BEGIN_HA3_NAMESPACE(search);

class ProviderBase
{
public:
    ProviderBase();
    virtual ~ProviderBase();
private:
    ProviderBase(const ProviderBase &);
    ProviderBase& operator=(const ProviderBase &);
public:
    // get aux table attribute reader.
    template <typename T>
    IE_NAMESPACE(partition)::AuxTableReaderTyped<T> *createAuxTableReader(const std::string &attrName);

    template <typename T>
    void deleteAuxTableReader(IE_NAMESPACE(partition)::AuxTableReaderTyped<T> *auxTableReader);

    // find global value
    template<typename T>
    T *findGlobalVariable(const std::string &variName) const;

    // for plugin set error message for display.
    void setErrorMessage(const std::string &errorMsg);

    const rank::GlobalMatchData *getGlobalMatchData() const;

    const common::Request *getRequest() const;

    void getQueryTermMetaInfo(rank::MetaInfo *metaInfo) const;

    common::AggResultReader getAggResultReader() const;

    common::AggregateResultsPtr getAggResults() const {
        return _aggResultsPtr;
    }
    common::QueryTerminator *getQueryTerminator();

    const matchdoc::Reference<rank::MatchData> *requireMatchData();
    const matchdoc::Reference<rank::SimpleMatchData> *requireSimpleMatchData();
    const matchdoc::Reference<rank::SimpleMatchData> *requireSubSimpleMatchData();
    const matchdoc::Reference<rank::MatchValues> *requireMatchValues();

    // using this interface is not recommended.
    // as we do not guarantee the compability of SearchPluginResource
    SearchPluginResource *getSearchPluginResource() const;
public:
    // for ha3
    void setAggregateResultsPtr(const common::AggregateResultsPtr &aggResultsPtr) {
        _aggResultsPtr = aggResultsPtr;
    }

    const std::string &getErrorMessage() const {
        return _errorMsg;
    }

    common::DataProvider *getDataProvider() const {
        return _searchPluginResource->dataProvider;
    }

    static int32_t convertRankTraceLevel(const common::Request *request);

    void setAttributeExprScope(AttributeExprScope scope) { _scope = scope; }
    AttributeExprScope getAttributeExprScope() { return _scope; }
protected:
    void setSearchPluginResource(SearchPluginResource *resource);
    template<typename T>
    T *doDeclareGlobalVariable(const std::string &variName, bool needSerizlize);

private:
    SearchPluginResource *_searchPluginResource;
    std::string _errorMsg;
    common::AggregateResultsPtr _aggResultsPtr;
    AttributeExprScope _scope;
private:
    friend class ProviderBaseTest;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(ProviderBase);

template <typename T>
IE_NAMESPACE(partition)::AuxTableReaderTyped<T> *ProviderBase::createAuxTableReader(const std::string &attrName) {
    auto partitionReaderSnapshot = _searchPluginResource->partitionReaderSnapshot;

    if (!partitionReaderSnapshot) {
        return NULL;
    }
    
    std::string tableName;
    tableName = partitionReaderSnapshot->GetTableNameByAttribute(attrName, tableName);
    return partitionReaderSnapshot->CreateAuxTableReader<T>(attrName,
            tableName, _searchPluginResource->pool);
}


template <typename T>
void ProviderBase::deleteAuxTableReader(IE_NAMESPACE(partition)::AuxTableReaderTyped<T> *auxTableReader) {
    heavenask::indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_searchPluginResource->pool, auxTableReader);
}


template<typename T>
inline T *ProviderBase::doDeclareGlobalVariable(
        const std::string &variName, bool needSerizlize)
{
    assert(_searchPluginResource->dataProvider);
    return _searchPluginResource->dataProvider->declareGlobalVariable<T>(variName, needSerizlize);
}

template<typename T>
inline T *ProviderBase::findGlobalVariable(const std::string &variName) const {
    assert(_searchPluginResource->dataProvider);
    return _searchPluginResource->dataProvider->findGlobalVariable<T>(variName);
}

END_HA3_NAMESPACE(search);

#endif //ISEARCH_PROVIDERBASE_H

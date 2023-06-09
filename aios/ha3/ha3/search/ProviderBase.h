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

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <string>

#include "indexlib/misc/common.h"
#include "indexlib/util/PoolUtil.h"
#include "indexlib/partition/partition_reader_snapshot.h"

#include "ha3/common/AggResultReader.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/DataProvider.h" // IWYU pragma: keep
#include "ha3/isearch.h"
#include "ha3/search/SearchPluginResource.h"
#include "ha3/search/MatchData.h"
#include "ha3/search/MatchValues.h"
#include "ha3/search/MetaInfo.h"
#include "ha3/search/SimpleMatchData.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace indexlib {
namespace partition {
template <typename T> class AuxTableReaderTyped;
}  // namespace partition
}  // namespace indexlib
namespace isearch {
namespace common {
class Request;
}  // namespace common
namespace rank {
class GlobalMatchData;
}  // namespace rank
}  // namespace isearch
namespace matchdoc {
template <typename T> class Reference;
}  // namespace matchdoc

namespace isearch {
namespace search {

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
    indexlib::partition::AuxTableReaderTyped<T> *createAuxTableReader(const std::string &attrName);

    template <typename T>
    void deleteAuxTableReader(indexlib::partition::AuxTableReaderTyped<T> *auxTableReader);

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
    common::TimeoutTerminator *getQueryTerminator();

    matchdoc::Reference<rank::MatchData> *requireMatchData();
    matchdoc::Reference<rank::SimpleMatchData> *requireSimpleMatchData();
    matchdoc::Reference<rank::SimpleMatchData> *requireSubSimpleMatchData();
    matchdoc::Reference<rank::MatchValues> *requireMatchValues();

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
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ProviderBase> ProviderBasePtr;

template <typename T>
indexlib::partition::AuxTableReaderTyped<T> *ProviderBase::createAuxTableReader(const std::string &attrName) {
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
void ProviderBase::deleteAuxTableReader(indexlib::partition::AuxTableReaderTyped<T> *auxTableReader) {
    indexlib::IE_POOL_COMPATIBLE_DELETE_CLASS(_searchPluginResource->pool, auxTableReader);
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

} // namespace search
} // namespace isearch

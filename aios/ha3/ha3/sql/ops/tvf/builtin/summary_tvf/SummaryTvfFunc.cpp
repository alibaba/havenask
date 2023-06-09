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
#include "ha3/sql/ops/tvf/builtin/SummaryTvfFunc.h"

#include <cstddef>
#include <memory>
#include <typeinfo>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/ConstString.h"
#include "autil/ConstStringUtil.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/DocIdClause.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/KVPairClause.h"
#include "ha3/common/Query.h"
#include "ha3/common/QueryClause.h"
#include "ha3/common/QueryTermVisitor.h"
#include "ha3/common/SummaryHit.h"
#include "ha3/queryparser/RequestParser.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/sql/common/Log.h"
#include "ha3/sql/ops/condition/ExprUtil.h"
#include "ha3/sql/ops/tvf/TvfFunc.h"
#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"
#include "ha3/sql/ops/tvf/TvfResourceBase.h"
#include "ha3/sql/ops/tvf/builtin/TvfSummaryResource.h"
#include "ha3/sql/ops/util/KernelUtil.h"
#include "ha3/summary/SummaryExtractorChain.h"
#include "ha3/summary/SummaryExtractorProvider.h"
#include "ha3/summary/SummaryProfile.h"
#include "ha3/summary/SummaryProfileManager.h"
#include "indexlib/document/index_document/normal_document/search_summary_document.h"
#include "matchdoc/ValueType.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/expression/util/FieldInfos.h"
#include "suez/turing/expression/util/SummaryInfo.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/util/TypeTransformer.h"
#include "table/ColumnSchema.h"
#include "table/ValueTypeSwitch.h"

namespace isearch {
namespace config {
class SqlTvfProfileInfo;
} // namespace config
} // namespace isearch

namespace table {
template <typename T>
class ColumnData;
} // namespace table

using namespace std;
using namespace autil;
using namespace table;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::queryparser;
using namespace isearch::summary;
using namespace isearch::search;
using namespace isearch::config;

namespace isearch {
namespace sql {
const string summaryTvfDef = R"tvf_json(
{
  "function_version": 1,
  "function_name": "summaryTvf",
  "function_type": "TVF",
  "is_deterministic": 1,
  "function_content_version": "json_default_0.1",
  "function_content": {
    "properties": {
      "enable_shuffle": false
    },
    "prototypes": [
      {
        "params": {
          "scalars": [
             {
               "type":"string"
             },
             {
               "type":"string"
             }
         ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        },
        "returns": {
          "new_fields": [
          ],
          "tables": [
            {
              "auto_infer": true
            }
          ]
        }
      }
    ]
  }
}
)tvf_json";

SummaryTvfFunc::SummaryTvfFunc()
    : _summaryExtractorChain(nullptr)
    , _summaryExtractorProvider(nullptr)
    , _summaryProfile(nullptr)
    , _summaryQueryInfo(nullptr)
    , _resource(nullptr)
    , _queryPool(nullptr) {}

SummaryTvfFunc::~SummaryTvfFunc() {
    if (_summaryExtractorChain != nullptr) {
        _summaryExtractorChain->endRequest();
        delete _summaryExtractorChain;
    }
    DELETE_AND_SET_NULL(_summaryExtractorProvider);
    DELETE_AND_SET_NULL(_summaryQueryInfo);
    DELETE_AND_SET_NULL(_resource);
}

bool SummaryTvfFunc::init(TvfFuncInitContext &context) {
    _queryPool = context.queryPool;
    _outputFields = context.outputFields;
    _outputFieldsType = context.outputFieldsType;
    auto tvfSummaryResource
        = context.tvfResourceContainer->get<TvfSummaryResource>("TvfSummaryResource");
    if (tvfSummaryResource == nullptr) {
        SQL_LOG(ERROR, "no tvf summary resource");
        return false;
    }
    auto summaryProfileMgrPtr = tvfSummaryResource->getSummaryProfileManager();
    if (summaryProfileMgrPtr == nullptr) {
        SQL_LOG(ERROR, "get summary profile manager failed");
        return false;
    }
    _summaryProfile = summaryProfileMgrPtr->getSummaryProfile(getName());
    if (_summaryProfile == nullptr) {
        SQL_LOG(ERROR, "not find summary_profile_name [%s]", getName().c_str());
        return false;
    }
    SQL_LOG(DEBUG, "use summary_profile_name [%s]", getName().c_str());
    if (!prepareResource(context)) {
        return false;
    }
    return true;
}

bool SummaryTvfFunc::prepareResource(TvfFuncInitContext &context) {
    if (context.params.size() != 2) {
        SQL_LOG(ERROR, "summary tvf need 2 params");
        return false;
    }
    _request.reset(new Request());
    auto *configClause = new ConfigClause();
    _request->setConfigClause(configClause);
    auto *kvpairClause = new KVPairClause();
    kvpairClause->setOriginalString(context.params[0]);
    _request->setKVPairClause(kvpairClause);
    auto *queryClause = new QueryClause();
    queryClause->setOriginalString(context.params[1]);
    _request->setQueryClause(queryClause);

    RequestParser requestParser;
    if (!requestParser.parseKVPairClause(_request)) {
        SQL_LOG(ERROR,
                "parse kvpair failed, error msg [%s]",
                requestParser.getErrorResult().getErrorMsg().c_str());
        return false;
    }
    if (!context.params[1].empty()
        && !requestParser.parseQueryClause(_request, *context.queryInfo)) {
        SQL_LOG(ERROR,
                "parse query failed, error msg [%s]",
                requestParser.getErrorResult().getErrorMsg().c_str());
        return false;
    }

    DocIdClause *docIdClause = new DocIdClause();
    docIdClause->setQueryString(context.params[1]);
    QueryTermVisitor termVisitor;
    if (queryClause->getQueryCount() > 1) {
        SQL_LOG(ERROR, "not support layer query");
    }
    Query *query = queryClause->getRootQuery();
    if (query) {
        query->accept(&termVisitor);
    }
    docIdClause->setTermVector(termVisitor.getTermVector());
    _request->setDocIdClause(docIdClause);

    _summaryQueryInfo = new SummaryQueryInfo(context.params[1], docIdClause->getTermVector());

    common::Ha3MatchDocAllocatorPtr matchDocAllocator(
        new common::Ha3MatchDocAllocator(_queryPool, false));
    _resource = new SearchCommonResource(nullptr,
                                         {},
                                         nullptr,
                                         nullptr,
                                         nullptr,
                                         nullptr,
                                         {},
                                         {},
                                         context.suezCavaAllocator,
                                         {},
                                         matchDocAllocator);
    return true;
}

void SummaryTvfFunc::toSummarySchema(const TablePtr &input) {
    TableInfo tableInfo;
    FieldInfos *fieldInfos = new FieldInfos();
    SummaryInfo *summaryInfo = new SummaryInfo();
    for (size_t i = 0; i < input->getColumnCount(); i++) {
        FieldInfo *fieldInfo = new FieldInfo();
        const string &fieldName = input->getColumnName(i);
        fieldInfo->fieldName = fieldName;
        matchdoc::ValueType vt = input->getColumnType(i);
        fieldInfo->isMultiValue = vt.isMultiValue();
        fieldInfo->fieldType = TypeTransformer::transform(vt.getBuiltinType());
        fieldInfos->addFieldInfo(fieldInfo);
        summaryInfo->addFieldName(fieldName);
    }
    tableInfo.setFieldInfos(fieldInfos);
    tableInfo.setSummaryInfo(summaryInfo);
    _hitSummarySchema.reset(new HitSummarySchema(&tableInfo));
}

void SummaryTvfFunc::toSummaryHits(const TablePtr &input, vector<SummaryHit *> &summaryHits) {
    for (size_t i = 0; i < input->getRowCount(); ++i) {
        SummaryHit *summaryHit = new SummaryHit(_hitSummarySchema.get(), _queryPool);
        auto summaryDoc = summaryHit->getSummaryDocument();
        for (size_t j = 0; j < input->getColumnCount(); ++j) {
            auto column = input->getColumn(j);
            const string &fieldValue = column->toOriginString(i);
            summaryDoc->SetFieldValue(j, fieldValue.c_str(), fieldValue.size());
        }
        summaryHits.push_back(summaryHit);
    }
}

template <typename T>
bool SummaryTvfFunc::fillSummaryDocs(Column *column,
                                     vector<SummaryHit *> &summaryHits,
                                     const TablePtr &input,
                                     summaryfieldid_t summaryFieldId) {
    ColumnData<T> *data = column->getColumnData<T>();
    for (size_t i = 0; i < summaryHits.size(); ++i) {
        const StringView *value
            = summaryHits[i]->getSummaryDocument()->GetFieldValue(summaryFieldId);
        T val;
        if (value == nullptr || value->size() == 0) {
            // default construct
            InitializeIfNeeded<T>()(val);
        } else {
            if (!ConstStringUtil::fromString(value, val, _queryPool)) {
                SQL_LOG(ERROR,
                        "summary value [%s], size [%ld], type [%s] convert failed",
                        value->to_string().c_str(),
                        value->size(),
                        typeid(val).name());
                InitializeIfNeeded<T>()(val);
            }
        }
        data->set(i, val);
    }
    return true;
}

bool SummaryTvfFunc::fromSummaryHits(const TablePtr &input, vector<SummaryHit *> &summaryHits) {
    input->clearRows();
    for (size_t i = 0; i < _outputFields.size(); ++i) {
        if (!input->declareColumn(_outputFields[i],
                                  ExprUtil::transSqlTypeToVariableType(_outputFieldsType[i]).first,
                                  false,
                                  false)) {
            SQL_LOG(ERROR,
                    "declare column for field [%s], type [%s] failed",
                    _outputFields[i].c_str(),
                    _outputFieldsType[i].c_str());
            return false;
        }
    }
    input->batchAllocateRow(summaryHits.size());
    for (size_t i = 0; i < input->getColumnCount(); ++i) {
        auto column = input->getColumn(i);
        ColumnSchema *columnSchema = column->getColumnSchema();
        const string &columnName = columnSchema->getName();
        summaryfieldid_t summaryFieldId = _hitSummarySchema->getSummaryFieldId(columnName);
        if (summaryFieldId == INVALID_SUMMARYFIELDID) {
            SQL_LOG(ERROR, "get summary field [%s] failed", columnName.c_str());
            return false;
        }
        auto vt = columnSchema->getType();
        auto func = [&](auto a) {
            typedef typename decltype(a)::value_type T;
            return this->fillSummaryDocs<T>(column, summaryHits, input, summaryFieldId);
        };
        if (!ValueTypeSwitch::switchType(vt, func, func)) {
            SQL_LOG(ERROR, "fill summary docs column[%s] failed", columnName.c_str());
            return false;
        }
    }
    return true;
}

bool SummaryTvfFunc::compute(const TablePtr &input, bool eof, TablePtr &output) {
    if (_hitSummarySchema == nullptr) {
        toSummarySchema(input);
        _summaryExtractorProvider
            = new SummaryExtractorProvider(_summaryQueryInfo,
                                           &_summaryProfile->getFieldSummaryConfig(),
                                           _request.get(),
                                           nullptr,                 // attr expr creator not need
                                           _hitSummarySchema.get(), // will modify schema
                                           *_resource);

        _summaryExtractorChain = _summaryProfile->createSummaryExtractorChain();
        if (!_summaryExtractorChain->beginRequest(_summaryExtractorProvider)) {
            return false;
        }
    }
    vector<SummaryHit *> summaryHits;
    toSummaryHits(input, summaryHits);
    for (auto summaryHit : summaryHits) {
        _summaryExtractorChain->extractSummary(*summaryHit);
    }
    if (!fromSummaryHits(input, summaryHits)) {
        return false;
    }
    output = input;
    for (auto summaryHit : summaryHits) {
        DELETE_AND_SET_NULL(summaryHit);
    }
    return true;
}

SummaryTvfFuncCreator::SummaryTvfFuncCreator()
    : TvfFuncCreatorR(summaryTvfDef) {}

SummaryTvfFuncCreator::~SummaryTvfFuncCreator() {}

TvfFunc *SummaryTvfFuncCreator::createFunction(const SqlTvfProfileInfo &info) {
    return new SummaryTvfFunc();
}

REGISTER_RESOURCE(SummaryTvfFuncCreator);

} // namespace sql
} // namespace isearch

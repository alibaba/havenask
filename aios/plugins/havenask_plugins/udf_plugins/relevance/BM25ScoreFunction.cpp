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
#include "havenask_plugins/udf_plugins/relevance/BM25ScoreFunction.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"

#include "autil/StringUtil.h"
#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "alog/Logger.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "matchdoc/Trait.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/framework/AttributeExpression.h"
#include "suez/turing/expression/framework/VariableTypeTraits.h"
#include "suez/turing/expression/framework/ArgumentAttributeExpression.h"
#include "suez/turing/expression/provider/TermMetaInfo.h"
#include "suez/turing/expression/provider/MetaInfo.h"
#include "suez/turing/expression/provider/MatchValues.h"
#include "havenask_plugins/udf_plugins/relevance/evaluators/TextRelEvaluator.h"
#include "ha3/search/MatchData.h"


using namespace matchdoc;

using namespace suez::turing;
USE_HAVENASK_UDF_NAMESPACE(evaluator);
USE_HAVENASK_UDF_NAMESPACE(relevance);
BEGIN_HAVENASK_UDF_NAMESPACE(bm25score);
AUTIL_LOG_SETUP(bm25score, BM25ScoreFunction);

BM25ScoreFunction::BM25ScoreFunction(const FunctionSubExprVec & exprs) : _exprs(exprs) {

}

BM25ScoreFunction::~BM25ScoreFunction() {
    if(_fieldMetaHelper != nullptr) {
        delete _fieldMetaHelper;
    }
}

bool BM25ScoreFunction::beginRequest(FunctionProvider* functionProvider) {
    assert(functionProvider);
    _functionProvider = functionProvider;
    if(_exprs.size() == 1) {
        auto* arg = GET_EXPR_IF_ARGUMENT(_exprs[0]);
        if (!arg || !arg->getConstValue<std::string>(_scoreIndexName)) {
            SQL_LOG(WARN, "invalid arg expression for in function: %s.",
               _exprs[0]->getOriginalString().c_str());
            return false;
        }
        SQL_LOG(DEBUG, "init score param, index name: %s", _scoreIndexName.c_str());
    }

    _matchInfoReader = _functionProvider->getMatchInfoReader();
    if(_matchInfoReader == nullptr) {
        SQL_LOG(ERROR, "failed to get matchInfo");
        return false;
    }

    _metaInfo = _matchInfoReader->getMetaInfo();
    if(_metaInfo == nullptr) {
        SQL_LOG(ERROR, "failed to get metaInfo");
        return false;
    }

    _matchDataRef = _matchInfoReader->getMatchDataRef();
    if(_matchDataRef == nullptr) {
        SQL_LOG(ERROR, "get matchData reference failed");
        return false;
    }
    if(!initMetas()) {
        return false;
    }
    return true;
}


bool BM25ScoreFunction::initMetas() {
    _fieldMetaHelper = new FieldMetaHelper();
    if(!_fieldMetaHelper->init(_functionProvider)){
        return false;
    }

    _partTotalDocCount = _matchInfoReader->getMetaInfo()->getPartTotalDocCount();
    SQL_LOG(DEBUG, "init score param, partition total doc count: %ld", _partTotalDocCount);

    for(size_t i=0;i<_metaInfo->size();i++) {
        auto& termMetaInfo = (*_metaInfo)[i];
        std::string termIndexName = termMetaInfo.getIndexName();
        // 过滤符合udf index参数的用户query term
        if(_scoreIndexName.length() != 0 && _scoreIndexName != termIndexName) {
            continue;
        }
        _filteredTermMetaIdx.push_back(i);
        
        double avgLength = 0.0;
        bool succ = _fieldMetaHelper->getIndexFieldSumAverageLength(avgLength, termIndexName);
        if(!succ) {
            return false;
        }
        // 保存query term对应的index的平均长度，避免重复计算
        _metaInfoIdxToAvgLength[i] = avgLength;
    }
    return true;
}

void BM25ScoreFunction::endRequest() {

}

bool BM25ScoreFunction::getIndexLengthOfDoc(int32_t docid, std::string indexName, int64_t& docLength, std::map<std::string, int64_t>& indexNameToIndexLength) {
    // 保存某个文档的某个index长度，避免重复计算
    if(indexNameToIndexLength.find(indexName) == indexNameToIndexLength.end()) {
        bool succ = _fieldMetaHelper->getIndexFieldSumLengthOfDoc(docid, docLength, indexName);
        if(!succ) {
            return false;
        }
        indexNameToIndexLength[indexName] = docLength;
    }
    else {
        docLength = indexNameToIndexLength[indexName];
    }
    return true;
}

float BM25ScoreFunction::evaluate(matchdoc::MatchDoc matchDoc) {
    BM25RelEvaluator evaluator(_partTotalDocCount);
    const MatchData &data = _matchDataRef->getReference(matchDoc);
    std::map<std::string, int64_t> indexNameToIndexLength;

    for(size_t k=0;k<_filteredTermMetaIdx.size();k++) {
        size_t metaInfoIdx = _filteredTermMetaIdx[k];
        auto& termMatchData = data.getTermMatchData(metaInfoIdx);
        if (termMatchData.isMatched()) {
            auto& termMetaInfo = (*_metaInfo)[metaInfoIdx];
            std::string termIndexName = termMetaInfo.getIndexName();
            int64_t docLength;
            bool succ = getIndexLengthOfDoc(matchDoc.getDocId(), termIndexName, docLength, indexNameToIndexLength);
            if(!succ) {
                return 0.0;
            }
            double avgLength = _metaInfoIdxToAvgLength[metaInfoIdx];
            int64_t termCount = termMatchData.getTermFreq();
            int64_t docCount = termMetaInfo.getDocFreq();
            SQL_LOG(DEBUG, "docid: [%d] bm25 term: [%s]", matchDoc.getDocId(), termMetaInfo.getTermText().c_str());
            evaluator.addTermParam(termCount, docCount, avgLength, docLength);
        }
    }
    return evaluator.getScore();
}


AUTIL_LOG_SETUP(bm25score, BM25ScoreFunctionCreatorImpl);

FunctionInterface* BM25ScoreFunctionCreatorImpl::createFunction(
        const FunctionSubExprVec& funcSubExprVec) {
    auto argSize = funcSubExprVec.size();
    if (argSize >1) {
        SQL_LOG(ERROR, "function should have zero or one args")
        return nullptr;
    }

    if (argSize ==1) {
        if(true == funcSubExprVec[0]->isMultiValue() || funcSubExprVec[0]->getType() != vt_string)
        {
            SQL_LOG(ERROR, "first arg must be string type");
            return nullptr;
        }
    }
    return new BM25ScoreFunction(funcSubExprVec);
}

END_HAVENASK_UDF_NAMESPACE(bm25score);

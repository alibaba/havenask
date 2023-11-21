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
#include "ha3/search/SimpleMatchData.h"


using namespace matchdoc;

using namespace suez::turing;
USE_HAVENASK_UDF_NAMESPACE(evaluator);
BEGIN_HAVENASK_UDF_NAMESPACE(relevance);

AUTIL_LOG_SETUP(relevance, FieldMetaHelper);

bool FieldMetaHelper::init(suez::turing::FunctionProvider* functionProvider)
{
    _indexInfoHelper = functionProvider->getIndexInfoHelper();
    if(_indexInfoHelper == nullptr) {
        SQL_LOG(ERROR, "failed to get index info helper");
        return false;
    }

    _fieldMetaReaderWrapper = functionProvider->getFieldMetaReaderWrapper();
    if(_fieldMetaReaderWrapper == nullptr) {
        SQL_LOG(ERROR, "failed to get field meta reader wrapper");
        return false;
    }
    return true;
}

const std::vector<std::string> FieldMetaHelper::getIndexFields(std::string indexName) {
    std::vector<std::string> indexFields = _indexInfoHelper->getFieldList(indexName);
    return indexFields;
}


bool FieldMetaHelper::getIndexFieldSumAverageLength(double& indexFieldSumAvgLength, std::string indexName) {
    std::vector<std::string>indexFields = getIndexFields(indexName);
    indexFieldSumAvgLength = 0.0;
    for(auto indexField: indexFields) {
        double fieldAvgLength = 0.0;
        bool succ = _fieldMetaReaderWrapper->getFieldAvgLength(fieldAvgLength, indexField);
        if(!succ) {
            SQL_LOG(ERROR, "get field average length meta [%s] failed for index [%s]", indexField.c_str(), indexName.c_str());
            return false;
        }
        // else {
        //     SQL_LOG(DEBUG, "get field: %s field average length: %lf", indexField.c_str(), fieldAvgLength);
        // }
        indexFieldSumAvgLength += fieldAvgLength;
    }
    return true;
}

bool FieldMetaHelper::getIndexFieldSumLengthOfDoc(int32_t docId, int64_t& fieldSumLengthOfDoc, std::string indexName) {
    std::vector<std::string> indexFields = getIndexFields(indexName);
    fieldSumLengthOfDoc = 0;
    for(auto indexField: indexFields) {
        int64_t fieldLength = 0;
        bool succ = _fieldMetaReaderWrapper->getFieldLengthOfDoc(docId, fieldLength, indexField);
        if(!succ) {
            SQL_LOG(ERROR, "get doc field length meta [%s] failed for index [%s]", indexField.c_str(), indexName.c_str());
            return false;
        }
        // else {
        //     SQL_LOG(DEBUG, "get docid: %d field: %s field length: %ld", docId, indexField.c_str(), fieldLength);
        // }
        fieldSumLengthOfDoc += fieldLength;
    }
    return true;
}

END_HAVENASK_UDF_NAMESPACE(relevance);

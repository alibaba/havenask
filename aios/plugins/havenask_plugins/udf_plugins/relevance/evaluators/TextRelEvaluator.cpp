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
#include "TextRelEvaluator.h"

BEGIN_HAVENASK_UDF_NAMESPACE(evaluator);

AUTIL_LOG_SETUP(evaluator, TfidfRelEvaluator);
AUTIL_LOG_SETUP(evaluator, BM25RelEvaluator);

void TfidfRelEvaluator::addTermParam(int64_t termFreq, int64_t docFreq) {
    docFreq = std::max(docFreq, 1L);
    float inverseDocFreq = log(1.0 * _totaldocFreq / docFreq);
    SQL_LOG(DEBUG, "add term parameter termFreq: %ld, docFreq: %ld, inverse doc frequency: %f", termFreq, docFreq, inverseDocFreq);
    _score += inverseDocFreq * termFreq;
}

void BM25RelEvaluator::addTermParam(int64_t termFreq, int64_t docFreq, double avgDocLength, int64_t docLength) {
    docFreq = std::max(docFreq, 1L);
    avgDocLength = std::max(avgDocLength, 1.0);
    docLength = std::max(docLength, 1L);
    float modifiedInverseDocFreq = log(1.0 + (_totaldocFreq - docFreq + 0.5) / (docFreq + 0.5));
    float modifiedTermFreq = (_k1 + 1.0) * termFreq / (termFreq + _k1 * (1 - _b + _b * docLength/avgDocLength));
    SQL_LOG(DEBUG, "add term parameter termFreq: %ld, docFreq: %ld, average doc length: %lf, doc length: %ld, modified inverse doc frequency: %f, modified term frequency: %f", 
                        termFreq, docFreq,  avgDocLength, docLength, modifiedInverseDocFreq, modifiedTermFreq);
    _score += modifiedInverseDocFreq * modifiedTermFreq;
}


END_HAVENASK_UDF_NAMESPACE(evaluator);


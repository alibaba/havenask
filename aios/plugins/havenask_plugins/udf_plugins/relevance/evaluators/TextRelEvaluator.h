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
#include <string>
#include <vector>

#include "autil/Log.h"
#include "suez/turing/expression/function/FunctionCreator.h"
#include "suez/turing/expression/function/FunctionInterface.h"
#include "suez/turing/expression/provider/MatchInfoReader.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"

#include "havenask_plugins/udf_plugins/utils/common.h"
#include "turing_ops_util/variant/Tracer.h"

namespace matchdoc {
class MatchDoc;
class ReferenceBase;
}  // namespace matchdoc
namespace suez {
namespace turing {
class FunctionProvider;
}  // namespace turing
}  // namespace suez

BEGIN_HAVENASK_UDF_NAMESPACE(evaluator);

class TfidfRelEvaluator {
public:
    TfidfRelEvaluator(int64_t totaldocFreq): _totaldocFreq(std::max(totaldocFreq,1L)) {
    }
    void addTermParam(int64_t termFreq, int64_t docFreq);
    float getScore() {
        return _score;
    }

private:
    float _score = 0;
    int64_t _totaldocFreq;

private:
    AUTIL_LOG_DECLARE();

};

class BM25RelEvaluator {
public:
    BM25RelEvaluator(
                int64_t totaldocFreq
                )
                : _totaldocFreq(std::max(totaldocFreq,1L)),
                _k1(1.2), _b(0.75)
                {
    }

    void addTermParam(int64_t termFreq, int64_t docFreq, double avgDocLength, int64_t docLength);
    float getScore() {
        return _score;
    }

private:
    float _score = 0;
    int64_t _totaldocFreq;
    float _k1;
    float _b;

private:
    AUTIL_LOG_DECLARE();
};

END_HAVENASK_UDF_NAMESPACE(evaluator);


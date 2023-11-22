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
#include "suez/turing/expression/util/FieldMetaReaderWrapper.h"
#include "suez/turing/expression/provider/FunctionProvider.h"
#include "suez/turing/expression/util/IndexInfoHelper.h"
#include "havenask_plugins/udf_plugins/utils/common.h"

namespace matchdoc {
class MatchDoc;
class ReferenceBase;
}  // namespace matchdoc
namespace suez {
namespace turing {
class FunctionProvider;
}  // namespace turing
}  // namespace suez

BEGIN_HAVENASK_UDF_NAMESPACE(relevance);

class FieldMetaHelper
{
public:
    FieldMetaHelper() {}
    ~FieldMetaHelper() {};
public:
    bool init(suez::turing::FunctionProvider* functionProvider);
    const std::vector<std::string> getIndexFields(std::string indexName);
    bool getIndexFieldSumAverageLength(double& indexFieldSumAvgLength, std::string indexName);
    bool getIndexFieldSumLengthOfDoc(int32_t docId, int64_t& fieldSumLengthOfDoc, std::string indexName);

private:
    std::shared_ptr<suez::turing::FieldMetaReaderWrapper> _fieldMetaReaderWrapper;
    const suez::turing::IndexInfoHelper* _indexInfoHelper;

private:
    AUTIL_LOG_DECLARE();
};

END_HAVENASK_UDF_NAMESPACE(relevance);


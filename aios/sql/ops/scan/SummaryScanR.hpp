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
#include "sql/ops/scan/SummaryScanR.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/Reference.h"
#include "sql/ops/util/KernelUtil.h"
#include "autil/ConstStringUtil.h"

namespace sql {

template <typename T>
bool SummaryScanR::fillSummaryDocs(const matchdoc::Reference<T> *ref,
                                   const SearchSummaryDocVecType &summaryDocs,
                                   const std::vector<matchdoc::MatchDoc> &matchDocs,
                                   int32_t summaryFieldId) {
    if (ref == nullptr) {
        SQL_LOG(ERROR, "reference is empty.");
        return false;
    }
    if (summaryDocs.size() != matchDocs.size()) {
        SQL_LOG(
            ERROR, "size not equal left [%ld], right [%ld].", summaryDocs.size(), matchDocs.size());
        return false;
    }
    for (size_t i = 0; i < matchDocs.size(); ++i) {
        const autil::StringView *value = summaryDocs[i]->GetFieldValue(summaryFieldId);
        if (value == nullptr) {
            SQL_LOG(ERROR, "summary get field id [%d] value failed", summaryFieldId);
            return false;
        }
        T val;
        if (value->size() == 0) {
            // default construct
            InitializeIfNeeded<T>()(val);
        } else {
            if (!autil::ConstStringUtil::fromString(value, val, _queryMemPoolR->getPool().get())) {
                SQL_LOG(TRACE2,
                        "summary value [%s], size [%ld], type [%s] convert failed",
                        value->to_string().c_str(),
                        value->size(),
                        typeid(val).name());
                InitializeIfNeeded<T>()(val);
            }
        }
        ref->set(matchDocs[i], val);
    }
    return true;
}

}

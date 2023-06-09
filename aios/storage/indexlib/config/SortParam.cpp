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
#include "indexlib/config/SortParam.h"

#include "indexlib/util/Exception.h"

using namespace std;

namespace indexlib::config {
AUTIL_LOG_SETUP(indexlib.config, SortParam);

void SortParam::fromSortDescription(const std::string sortDescription)
{
    if (sortDescription.empty()) {
        string errMsg = "sort description is empty";
        AUTIL_LOG(ERROR, "error msg: %s", errMsg.c_str());
        AUTIL_LEGACY_THROW(autil::legacy::ParameterInvalidException, errMsg);
    }

    if (sortDescription[0] == SORT_DESCRIPTION_DESCEND) {
        mSortField = sortDescription.substr(1);
        mSortPattern = indexlibv2::config::sp_desc;
    } else if (sortDescription[0] == SORT_DESCRIPTION_ASCEND) {
        mSortField = sortDescription.substr(1);
        mSortPattern = indexlibv2::config::sp_asc;
    } else {
        mSortField = sortDescription;
        mSortPattern = indexlibv2::config::sp_desc;
    }
}

std::string SortParam::toSortDescription() const
{
    stringstream ss;
    ss << (IsDesc() ? SORT_DESCRIPTION_DESCEND : SORT_DESCRIPTION_ASCEND);
    ss << mSortField;
    return ss.str();
}

void SortParam::AssertEqual(const SortParam& other) const
{
    if (mSortField != other.mSortField) {
        AUTIL_LEGACY_THROW(util::AssertEqualException, "mSortField doesn't match");
    }
    if (mSortPattern != other.mSortPattern) {
        AUTIL_LEGACY_THROW(util::AssertEqualException, "mSortPattern doesn't match");
    }
}

} // namespace indexlib::config

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

#include "autil/Log.h"
#include "autil/StringTokenizer.h"
#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Constant.h"
#include "indexlib/config/SortDescription.h"

namespace indexlib::config {

#define SORT_DESCRIPTION_SEPERATOR ";"
#define SORT_DESCRIPTION_DESCEND '-'
#define SORT_DESCRIPTION_ASCEND '+'

class SortParam : public autil::legacy::Jsonizable
{
public:
    SortParam() : mSortPattern(indexlibv2::config::sp_desc) {}

    ~SortParam() {}

public:
    std::string GetSortField() const { return mSortField; }

    void SetSortField(const std::string& sortField) { mSortField = sortField; }

    indexlibv2::config::SortPattern GetSortPattern() const { return mSortPattern; }

    void SetSortPattern(indexlibv2::config::SortPattern sortPattern) { mSortPattern = sortPattern; }

    std::string GetSortPatternString() const
    {
        return indexlibv2::config::SortDescription::SortPatternToString(mSortPattern);
    }

    void SetSortPatternString(const std::string& sortPatternString)
    {
        mSortPattern = indexlibv2::config::SortDescription::SortPatternFromString(sortPatternString);
    }

    bool IsDesc() const { return mSortPattern != indexlibv2::config::sp_asc; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override
    {
        json.Jsonize("SortField", mSortField, mSortField);
        if (json.GetMode() == FROM_JSON) {
            std::string strSortPattern;
            json.Jsonize("SortType", strSortPattern, strSortPattern);
            mSortPattern = indexlibv2::config::SortDescription::SortPatternFromString(strSortPattern);
        } else {
            std::string strSortPattern = indexlibv2::config::SortDescription::SortPatternToString(mSortPattern);
            json.Jsonize("SortType", strSortPattern, strSortPattern);
        }
    }

    void fromSortDescription(const std::string sortDescription);

    std::string toSortDescription() const;

    void AssertEqual(const SortParam& other) const;

private:
    std::string mSortField;
    indexlibv2::config::SortPattern mSortPattern;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::vector<SortParam> SortParams;

inline const std::string SortParamsToString(const SortParams& sortParams)
{
    if (sortParams.empty()) {
        return "";
    }

    std::stringstream ss;
    size_t i = 0;
    for (i = 0; i < sortParams.size() - 1; ++i) {
        ss << sortParams[i].toSortDescription() << SORT_DESCRIPTION_SEPERATOR;
    }
    ss << sortParams[i].toSortDescription();
    return ss.str();
}

inline SortParams StringToSortParams(const std::string& sortDesp)
{
    SortParams params;
    autil::StringTokenizer st(sortDesp, SORT_DESCRIPTION_SEPERATOR,
                              autil::StringTokenizer::TOKEN_TRIM | autil::StringTokenizer::TOKEN_IGNORE_EMPTY);
    for (autil::StringTokenizer::Iterator it = st.begin(); it != st.end(); it++) {
        SortParam sortParam;
        sortParam.fromSortDescription(*it);
        params.push_back(sortParam);
    }
    return params;
}
} // namespace indexlib::config

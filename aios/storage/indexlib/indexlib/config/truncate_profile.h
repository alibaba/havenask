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
#ifndef __INDEXLIB_TRUNCATE_PROFILE_H
#define __INDEXLIB_TRUNCATE_PROFILE_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/config/SortParam.h"
#include "indexlib/config/config_define.h"
#include "indexlib/config/truncate_profile_config.h"
#include "indexlib/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace config {

class TruncateProfile : public autil::legacy::Jsonizable
{
public:
    TruncateProfile();
    TruncateProfile(TruncateProfileConfig& truncateProfileConfig);
    ~TruncateProfile();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void Check() const;

    void AppendIndexName(const std::string& indexName);

    bool operator==(const TruncateProfile& other) const;

    bool HasSort() const { return !mSortParams.empty(); };

    size_t GetSortDimenNum() const { return mSortParams.size(); }

    bool IsSortByDocPayload() const;
    bool DocPayloadUseFP16() const;
    std::string GetDocPayloadFactorField() const;

public:
    static bool IsSortParamByDocPayload(const SortParam& sortParam);

public:
    typedef std::vector<std::string> StringVec;

    std::string mTruncateProfileName;
    SortParams mSortParams;
    std::string mTruncateStrategyName;
    StringVec mIndexNames;
    std::map<std::string, std::string> mDocPayloadParams;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(TruncateProfile);
}} // namespace indexlib::config

#endif //__INDEXLIB_TRUNCATE_PROFILE_H

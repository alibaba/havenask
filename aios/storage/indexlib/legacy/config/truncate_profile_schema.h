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

#include <memory>

#include "autil/Log.h"
#include "indexlib/config/truncate_profile_config.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {
class TruncateProfileSchemaImpl;
typedef std::shared_ptr<TruncateProfileSchemaImpl> TruncateProfileSchemaImplPtr;

class TruncateProfileSchema : public autil::legacy::Jsonizable
{
public:
    typedef std::map<std::string, TruncateProfileConfigPtr> TruncateProfileConfigMap;
    typedef TruncateProfileConfigMap::const_iterator Iterator;

public:
    TruncateProfileSchema();
    ~TruncateProfileSchema() {}

public:
    TruncateProfileConfigPtr GetTruncateProfileConfig(const std::string& truncateProfileName) const;
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const TruncateProfileSchema& other) const;

    Iterator Begin() const;
    Iterator End() const;
    size_t Size() const;

    const TruncateProfileConfigMap& GetTruncateProfileConfigMap();

    // for test
    void AddTruncateProfileConfig(const TruncateProfileConfigPtr& truncateProfileConfig);

private:
    TruncateProfileSchemaImplPtr mImpl;

private:
    AUTIL_LOG_DECLARE();
};
typedef std::shared_ptr<TruncateProfileSchema> TruncateProfileSchemaPtr;
}} // namespace indexlib::config

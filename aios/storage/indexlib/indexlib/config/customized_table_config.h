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
#ifndef __INDEXLIB_CUSTOMIZED_TABLE_CONFIG_H
#define __INDEXLIB_CUSTOMIZED_TABLE_CONFIG_H

#include <memory>

#include "autil/legacy/jsonizable.h"
#include "indexlib/common_define.h"
#include "indexlib/indexlib.h"

DECLARE_REFERENCE_CLASS(config, CustomizedTableConfigImpl);

namespace indexlib { namespace config {

class CustomizedTableConfig : public autil::legacy::Jsonizable
{
public:
    CustomizedTableConfig();
    CustomizedTableConfig(const CustomizedTableConfig& other);
    ~CustomizedTableConfig();

public:
    const std::string& GetPluginName() const;
    const std::string& GetIdentifier() const;
    bool GetParameters(const std::string& key, std::string& value) const;
    const std::map<std::string, std::string>& GetParameters() const;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const CustomizedTableConfig& other) const;

private:
    CustomizedTableConfigImplPtr mImpl;

private:
    IE_LOG_DECLARE();
};

DEFINE_SHARED_PTR(CustomizedTableConfig);
}} // namespace indexlib::config

#endif //__INDEXLIB_CUSTOMIZED_TABLE_CONFIG_H

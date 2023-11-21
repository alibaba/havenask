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
#include "autil/legacy/jsonizable.h"
#include "indexlib/legacy/indexlib.h"

namespace indexlib { namespace config {

class CustomizedTableConfigImpl : public autil::legacy::Jsonizable
{
public:
    CustomizedTableConfigImpl();
    CustomizedTableConfigImpl(const CustomizedTableConfigImpl& other);
    ~CustomizedTableConfigImpl();

public:
    const std::string& GetPluginName() const { return mPluginName; }
    const std::string& GetIdentifier() const { return mId; }
    bool GetParameters(const std::string& key, std::string& value) const;
    const std::map<std::string, std::string>& GetParameters() const { return mParameters; }

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void AssertEqual(const CustomizedTableConfigImpl& other) const;

private:
    std::string mId;
    std::string mPluginName;
    std::map<std::string, std::string> mParameters;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<CustomizedTableConfigImpl> CustomizedTableConfigImplPtr;
}} // namespace indexlib::config

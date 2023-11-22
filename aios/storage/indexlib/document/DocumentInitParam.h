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

#include <map>
#include <string>

namespace indexlibv2::document {

class DocumentInitParam
{
public:
    typedef std::map<std::string, std::string> KeyValueMap;

public:
    DocumentInitParam() {}
    DocumentInitParam(const KeyValueMap& kvMap) : _kvMap(kvMap) {}
    virtual ~DocumentInitParam() {}

public:
    void AddValue(const std::string& key, const std::string& value);
    bool GetValue(const std::string& key, std::string& value);

protected:
    KeyValueMap _kvMap;
};

/////////////////////////////////////////////////////////////////

template <typename UserDefineResource>
class DocumentInitParamTyped : public DocumentInitParam
{
public:
    DocumentInitParamTyped() {}
    DocumentInitParamTyped(const KeyValueMap& kvMap, const UserDefineResource& res)
        : DocumentInitParam(kvMap)
        , _resource(res)
    {
    }

    ~DocumentInitParamTyped() {}

    void SetResource(const UserDefineResource& res) { _resource = res; }
    const UserDefineResource& GetResource() const { return _resource; }

    using DocumentInitParam::AddValue;
    using DocumentInitParam::GetValue;

private:
    UserDefineResource _resource;
};

} // namespace indexlibv2::document

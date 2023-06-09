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
#include <string_view>

namespace indexlibv2::framework {

class IndexRoot
{
public:
    IndexRoot() = default;
    IndexRoot(std::string localRoot, std::string remoteRoot)
        : _localRoot(std::move(localRoot))
        , _remoteRoot(std::move(remoteRoot))
    {
    }

    bool operator==(const IndexRoot& other) const
    {
        if (other._localRoot != _localRoot) {
            return false;
        }
        if (other._remoteRoot != _remoteRoot) {
            return false;
        }
        return true;
    }

    const std::string& GetLocalRoot() const { return _localRoot; }
    const std::string& GetRemoteRoot() const { return _remoteRoot; }

    std::string ToString() const { return std::string("local: ") + _localRoot + ", remote: " + _remoteRoot; }

private:
    std::string _localRoot;
    std::string _remoteRoot;
};

} // namespace indexlibv2::framework

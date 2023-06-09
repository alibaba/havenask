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

#include "autil/legacy/jsonizable.h"
#include "indexlib/base/Status.h"

namespace indexlib::file_system {
class Directory;
}

namespace indexlibv2::index {

class KVFormatOptions final : public autil::legacy::Jsonizable
{
public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

public:
    bool IsShortOffset() const { return _shortOffset; }
    bool UseCompactBucket() const { return _useCompactBucket; }

    void SetShortOffset(bool flag) { _shortOffset = flag; }
    void SetUseCompactBucket(bool flag) { _useCompactBucket = flag; }

    std::string ToString() const;
    void FromString(const std::string& str);

    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& directory);
    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& directory) const;

public:
    static Status Load(const std::shared_ptr<indexlib::file_system::Directory>& directory, KVFormatOptions& opts);
    static Status Store(const std::shared_ptr<indexlib::file_system::Directory>& directory,
                        const KVFormatOptions& opts);

private:
    bool _shortOffset = false;
    bool _useCompactBucket = false;
};

} // namespace indexlibv2::index

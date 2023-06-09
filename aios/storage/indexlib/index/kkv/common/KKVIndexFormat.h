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

#include "autil/legacy/jsonizable.h"
#include "indexlib/file_system/IDirectory.h"

namespace indexlibv2::index {

class KKVIndexFormat : public autil::legacy::Jsonizable
{
public:
    KKVIndexFormat();
    KKVIndexFormat(bool storeTs, bool keepSortSeq, bool valueInline);
    ~KKVIndexFormat();

public:
    Status Store(const std::shared_ptr<indexlib::file_system::IDirectory>& directory);
    Status Load(const std::shared_ptr<indexlib::file_system::IDirectory>& directory);

    std::string ToString();
    void FromString(const std::string& content);
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool StoreTs() const { return _storeTs; }
    bool KeepSortSequence() const { return _keepSortSeq; }
    bool ValueInline() const { return _valueInline; }

private:
    bool _storeTs;
    bool _keepSortSeq;
    bool _valueInline;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::index

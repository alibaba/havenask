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
#include "autil/legacy/jsonizable.h"
#include "indexlib/framework/Version.h"
#include "indexlib/framework/index_task/IndexTaskContext.h"
#include "indexlib/framework/index_task/IndexTaskResource.h"

namespace indexlibv2::table {

class VersionResource : public framework::IndexTaskResource, public autil::legacy::Jsonizable
{
public:
    VersionResource(std::string name, framework::IndexTaskResourceType type);
    ~VersionResource();

public:
    Status Store(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;
    Status Load(const std::shared_ptr<indexlib::file_system::Directory>& resourceDirectory) override;

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;

    void SetVersion(const framework::Version& version) { _version = version; }
    const framework::Version& GetVersion() const { return _version; }

private:
    framework::Version _version;

    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::table

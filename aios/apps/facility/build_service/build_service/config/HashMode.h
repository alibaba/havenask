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
#ifndef ISEARCH_BS_HASHMODE_H
#define ISEARCH_BS_HASHMODE_H

#include "autil/Log.h"
#include "autil/legacy/jsonizable.h"

namespace build_service { namespace config {

class HashMode : public autil::legacy::Jsonizable
{
public:
    HashMode();
    virtual ~HashMode();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    virtual bool validate() const;

public:
    std::string _hashFunction;
    mutable std::vector<std::string> _hashFields;
    std::map<std::string, std::string> _hashParams;
    std::string _hashField;

private:
    AUTIL_LOG_DECLARE();
};

class RegionHashMode : public HashMode
{
public:
    RegionHashMode();
    ~RegionHashMode();

public:
    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    bool validate() const override;

public:
    std::string _regionName;

public:
    static const std::string REGION_NAME;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::vector<RegionHashMode> RegionHashModes;

}} // namespace build_service::config

#endif // ISEARCH_BS_HASHMODE_H

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

#include "indexlib/config/config_define.h"
#include "indexlib/config/diversity_constrain.h"
#include "indexlib/legacy/indexlib.h"
#include "indexlib/util/Exception.h"

namespace indexlib { namespace config {

class MetaReTruncateConfig : public autil::legacy::Jsonizable
{
public:
    MetaReTruncateConfig() : _limit(-1) {}

    void Jsonize(autil::legacy::Jsonizable::JsonWrapper& json) override;
    void Check() const;

    bool NeedReTruncate() const { return _limit > 0; }
    uint64_t GetLimit() const { return _limit; }
    const DiversityConstrain& GetDiversityConstrain() const { return _diversityConstrain; }

    bool operator==(const MetaReTruncateConfig& other) const;

public:
    void TEST_SetLimit(int64_t limit) { _limit = limit; }
    void TEST_SetDiversityConstrain(const DiversityConstrain& constrain) { _diversityConstrain = constrain; }

private:
    int64_t _limit;
    DiversityConstrain _diversityConstrain;
};

}} // namespace indexlib::config

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

#include "navi/engine/Resource.h"

namespace table {
class Table;
} // namespace table

namespace sql {

class HashJoinMapR : public navi::Resource {
public:
    HashJoinMapR();
    ~HashJoinMapR();
    HashJoinMapR(const HashJoinMapR &) = delete;
    HashJoinMapR &operator=(const HashJoinMapR &) = delete;

public:
    typedef std::vector<std::pair<size_t, size_t>> HashValues; // row : hash value

public:
    void def(navi::ResourceDefBuilder &builder) const override;
    navi::ErrorCode init(navi::ResourceInitContext &ctx) override;

public:
    void createHashMap(const HashValues &values);
    bool getHashValues(const std::shared_ptr<table::Table> &table,
                       size_t offset,
                       size_t count,
                       const std::vector<std::string> &columnName,
                       HashValues &values);

private:
    bool getColumnHashValues(const std::shared_ptr<table::Table> &table,
                             size_t offset,
                             size_t count,
                             const std::string &columnName,
                             HashValues &values);
    void combineHashValues(const HashValues &valuesA, HashValues &valuesB);

public:
    static const std::string RESOURCE_ID;

public:
    std::unordered_map<size_t, std::vector<size_t>> _hashJoinMap;
    bool _shouldClearTable = false;
};

} // namespace sql

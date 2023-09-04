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

#include "catalog/entity/CatalogSnapshot.h"
#include "catalog/store/CatalogState.h"
#include "catalog/store/ICatalogWriter.h"

namespace catalog {

class CatalogWriter : public ICatalogWriter {
public:
    CatalogWriter(const std::shared_ptr<CatalogState> &state);
    ~CatalogWriter();

public:
    Status write(CatalogSnapshot *oldSnapshot, CatalogSnapshot *newSnapshot) override;
    Status updateStatus(CatalogSnapshot *newSnapshot, const proto::EntityStatus &target) override;
    Status write(const std::vector<proto::Build> &builds) override;

private:
    std::shared_ptr<CatalogState> _state;
};

} // namespace catalog

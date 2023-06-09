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

#include "ha3/sql/ops/tvf/TvfFuncCreatorR.h"
#include "suez/turing/search/base/UserMetadata.h"

namespace isearch {
namespace sql {
class TvfFuncCreatorR;
}  // namespace sql

namespace turing {

class TvfMetadata : public suez::turing::Metadata {
public:
    TvfMetadata(const std::map<std::string, isearch::sql::TvfFuncCreatorR *> &tvfNameToCreator)
        : _tvfNameToCreator(tvfNameToCreator)
    {
    }
public:
    static const std::string META_TYPE;
public:
    const std::string &getMetaType() const override {
        return META_TYPE;
    }
    const std::map<std::string, isearch::sql::TvfFuncCreatorR *> &getTvfNameToCreator() const {
        return _tvfNameToCreator;
    }
private:
    std::map<std::string, isearch::sql::TvfFuncCreatorR *> _tvfNameToCreator;
};

} // namespace turing
} // namespace isearch

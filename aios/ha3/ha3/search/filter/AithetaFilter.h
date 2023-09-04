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

#include "indexlib/index/ann/aitheta2/AithetaAuxSearchInfo.h"

namespace isearch {
namespace search {

class AithetaFilter : public indexlibv2::index::ann::AithetaFilterBase {
public:
    AithetaFilter() = default;
    ~AithetaFilter() = default;

public:
    template <typename T>
    void SetFilter(T &&func) {
        mFilterImpl = std::forward<T>(func);
    }
    bool operator()(docid_t docId) const override {
        return mFilterImpl(docId);
    }
    std::string ToString() const override {
        return "";
    }

private:
    std::function<bool(docid_t)> mFilterImpl;
};

} // namespace search
} // namespace isearch

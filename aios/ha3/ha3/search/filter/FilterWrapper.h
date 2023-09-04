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
#include <stddef.h>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/Filter.h"
#include "ha3/search/JoinFilter.h"
#include "matchdoc/MatchDoc.h"

namespace matchdoc {
class SubDocAccessor;
} // namespace matchdoc

namespace isearch {
namespace search {

class SubDocFilter {
public:
    SubDocFilter(matchdoc::SubDocAccessor *accessor)
        : _subDocAccessor(accessor) {}

public:
    bool pass(matchdoc::MatchDoc matchDoc);

private:
    matchdoc::SubDocAccessor *_subDocAccessor;
};

class FilterWrapper {
public:
    FilterWrapper();
    virtual ~FilterWrapper();

private:
    FilterWrapper(const FilterWrapper &);
    FilterWrapper &operator=(const FilterWrapper &);

public:
    inline bool pass(matchdoc::MatchDoc matchDoc);

public:
    void setFilter(Filter *filter) {
        if (!filter) {
            return;
        }
        if (filter->needFilterSubDoc()) {
            _subExprFilter = filter;
        } else {
            _filter = filter;
        }
    }
    Filter *getFilter() const {
        return _filter;
    }
    Filter *getSubExprFilter() const {
        return _subExprFilter;
    }

    void setJoinFilter(JoinFilter *joinFilter) {
        _joinFilter = joinFilter;
    }
    JoinFilter *getJoinFilter() const {
        return _joinFilter;
    }

    void setSubDocFilter(SubDocFilter *subDocFilter) {
        _subDocFilter = subDocFilter;
    }
    SubDocFilter *getSubDocFilter() const {
        return _subDocFilter;
    }
    virtual void resetFilter() {}

private:
    JoinFilter *_joinFilter;
    SubDocFilter *_subDocFilter;
    Filter *_filter;
    Filter *_subExprFilter;

private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<FilterWrapper> FilterWrapperPtr;

inline bool FilterWrapper::pass(matchdoc::MatchDoc matchDoc) {
    if (_joinFilter != NULL && !_joinFilter->pass(matchDoc)) {
        return false;
    }
    if (_subDocFilter != NULL && !_subDocFilter->pass(matchDoc)) {
        return false;
    }
    if (_filter != NULL && !_filter->pass(matchDoc)) {
        return false;
    }
    return true;
}

} // namespace search
} // namespace isearch

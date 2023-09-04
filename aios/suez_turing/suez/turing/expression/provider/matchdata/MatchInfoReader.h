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
#include <stdint.h>

namespace suez {
namespace turing {
class MatchValues;
class MetaInfo;
class SimpleMatchData;
class MatchData;
} // namespace turing
} // namespace suez
namespace matchdoc {
template <typename T>
class Reference;
class MatchDocAllocator;
} // namespace matchdoc

namespace suez {
namespace turing {

class MatchInfoReader {
public:
    MatchInfoReader();
    ~MatchInfoReader();

private:
    MatchInfoReader(const MatchInfoReader &);
    MatchInfoReader &operator=(const MatchInfoReader &);

public:
    void init(std::shared_ptr<MetaInfo> metaInfo, matchdoc::MatchDocAllocator *allocator);
    MetaInfo *getMetaInfo() { return _metaInfo.get(); }
    std::shared_ptr<MetaInfo> getMetaInfoPtr() { return _metaInfo; }
    const matchdoc::Reference<SimpleMatchData> *getSimpleMatchDataRef() { return _simpleMatchDataRef; }
    const matchdoc::Reference<MatchValues> *getMatchValuesRef() { return _matchValuesRef; }
    const matchdoc::Reference<MatchData> *getMatchDataRef() { return _matchDataRef; }

private:
    void reset();

private:
    std::shared_ptr<MetaInfo> _metaInfo;
    const matchdoc::Reference<SimpleMatchData> *_simpleMatchDataRef;
    const matchdoc::Reference<MatchData> *_matchDataRef;
    const matchdoc::Reference<MatchValues> *_matchValuesRef;
};

} // namespace turing
} // namespace suez

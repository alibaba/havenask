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

#include <set>
#include <stddef.h>

#include "suez/sdk/SourceReaderProvider.h"
#include "suez/sdk/TableReader.h"

namespace suez {

struct PartitionId;
class TableWriter;

class IndexProvider {
public:
    IndexProvider();
    ~IndexProvider();

public:
    bool operator==(const IndexProvider &other) const;
    bool operator!=(const IndexProvider &other) const;
    void erase(const PartitionId &pid);
    void clear();

    // for igraph
    bool mergeProviderAndGenErasePids(const IndexProvider &newIndexProvider, std::set<PartitionId> &pidsToErase);

private:
    bool mergeProvider(const IndexProvider &newIndexProvider);
    std::set<PartitionId> genErasePartitionIdSet(const IndexProvider &newIndexProvider) const;
    template <typename ReaderType>
    bool mergeTypedReader(const IndexProvider &newIndexProvider, const ReaderType &srcReader, ReaderType &targetReader);
    void mergeSourceReaderProvider(const SourceReaderProvider &newSourceReaderProvider);

public:
    // tableReader and tabletReader must correspond to multiTableReader.
    TableReader tableReader;
    TabletReader tabletReader;
    MultiTableReader multiTableReader;
    std::map<PartitionId, std::shared_ptr<TableWriter>> tableWriters;
    std::set<PartitionId> targetPids;
    SourceReaderProvider sourceReaderProvider;
    bool allTableReady;
};

using IndexProviderPtr = std::shared_ptr<IndexProvider>;

} // namespace suez

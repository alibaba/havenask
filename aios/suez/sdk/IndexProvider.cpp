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
#include "suez/sdk/IndexProvider.h"

#include <iosfwd>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "suez/sdk/PartitionId.h"
#include "suez/sdk/SourceReaderProvider.h"

using namespace std;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, IndexProvider);

bool IndexProvider::operator==(const IndexProvider &other) const {
    return (multiTableReader == other.multiTableReader) && (allTableReady == other.allTableReady);
}

bool IndexProvider::operator!=(const IndexProvider &other) const { return !(*this == other); }

IndexProvider::IndexProvider() : allTableReady(true) {}

IndexProvider::~IndexProvider() {}

template <typename ReaderType>
bool IndexProvider::mergeTypedReader(const IndexProvider &newIndexProvider,
                                     const ReaderType &srcReader,
                                     ReaderType &targetReader) {
    for (const auto &[pid, value] : srcReader) {
        targetReader[pid] = value;
        SingleTableReaderPtr reader = newIndexProvider.multiTableReader.getTableReader(pid);
        if (!reader) {
            return false;
        }
        this->multiTableReader.addTableReader(pid, reader);
    }
    return true;
}

bool IndexProvider::mergeProvider(const IndexProvider &newIndexProvider) {
    IndexProvider snapShot = *this;
    this->allTableReady = newIndexProvider.allTableReady;
    for (const auto &newWriter : newIndexProvider.tableWriters) {
        tableWriters[newWriter.first] = newWriter.second;
    }
    if (!mergeTypedReader(newIndexProvider, newIndexProvider.tableReader, this->tableReader) ||
        !mergeTypedReader(newIndexProvider, newIndexProvider.tabletReader, this->tabletReader)) {
        AUTIL_LOG(ERROR, "tableReader is not consistent with multiTableReader. may has a bug!");
        *this = snapShot;
        return false;
    }
    mergeSourceReaderProvider(newIndexProvider.sourceReaderProvider);
    return true;
}

void IndexProvider::mergeSourceReaderProvider(const SourceReaderProvider &newSourceReaderProvider) {
    sourceReaderProvider.swiftClientCreator = newSourceReaderProvider.swiftClientCreator;
    for (const auto &[tableName, config] : newSourceReaderProvider.sourceConfigs) {
        sourceReaderProvider.sourceConfigs[tableName] = config;
    }
}

set<PartitionId> IndexProvider::genErasePartitionIdSet(const IndexProvider &newIndexProvider) const {
    auto ret = setDiff(mapKeys2Set(this->tableReader), mapKeys2Set(newIndexProvider.tableReader));
    auto tabletDiff = setDiff(mapKeys2Set(this->tabletReader), mapKeys2Set(newIndexProvider.tabletReader));
    ret.insert(tabletDiff.begin(), tabletDiff.end());
    return ret;
}

void IndexProvider::erase(const PartitionId &pid) {
    tableReader.erase(pid);
    tabletReader.erase(pid);
    multiTableReader.erase(pid);
    tableWriters.erase(pid);
    sourceReaderProvider.erase(pid);
}

void IndexProvider::clear() {
    tableReader.clear();
    tabletReader.clear();
    multiTableReader.clear();
    tableWriters.clear();
    sourceReaderProvider.clear();
    allTableReady = false;
}

bool IndexProvider::mergeProviderAndGenErasePids(const IndexProvider &newIndexProvider,
                                                 std::set<PartitionId> &pidsToErase) {
    if (!mergeProvider(newIndexProvider)) {
        return false;
    }
    pidsToErase = genErasePartitionIdSet(newIndexProvider);
    return true;
}

} // namespace suez

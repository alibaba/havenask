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
#include "indexlib/table/kkv_table/KKVTabletSessionReader.h"

namespace indexlibv2::table {
AUTIL_LOG_SETUP(indexlib.table, KKVTabletSessionReader);

KKVTabletSessionReader::KKVTabletSessionReader(const std::shared_ptr<KKVTabletReader>& kkvTabletReader,
                                               const std::shared_ptr<framework::IIndexMemoryReclaimer>& memReclaimer)
    : CommonTabletSessionReader<KKVTabletReader>(kkvTabletReader, memReclaimer)
{
}

KKVTabletSessionReader::~KKVTabletSessionReader() {}

} // namespace indexlibv2::table

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

#include "indexlib/index/ann/aitheta2/impl/SegmentBuilder.h"

#include "indexlib/index/ann/aitheta2/util/PkDumper.h"

using namespace indexlib::file_system;
using namespace autil;
using namespace std;
using namespace aitheta2;
namespace indexlibv2::index::ann {

bool SegmentBuilder::DumpPrimaryKey(const PkDataHolder& pkDataHolder, Segment& segment)
{
    if (!_indexConfig.buildConfig.storePrimaryKey) {
        AUTIL_LOG(INFO, "not need to store primary key");
        return true;
    }
    PkDumper pkDumper;
    return pkDumper.Dump(pkDataHolder, segment);
}

AUTIL_LOG_SETUP(indexlib.index, SegmentBuilder);
} // namespace indexlibv2::index::ann

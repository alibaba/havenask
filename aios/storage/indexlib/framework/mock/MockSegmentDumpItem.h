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

#include "gmock/gmock.h"

#include "indexlib/base/Status.h"
#include "indexlib/framework/SegmentDumpItem.h"

namespace indexlibv2::framework {

class MockSegmentDumpItem : public SegmentDumpItem
{
public:
    MockSegmentDumpItem() {}
    ~MockSegmentDumpItem() {}

    MOCK_METHOD(Status, Dump, (), (noexcept, override));
    MOCK_METHOD(bool, IsDumped, (), (const, override));
};
} // namespace indexlibv2::framework

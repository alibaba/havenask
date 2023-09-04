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

#include "indexlib/framework/Tablet.h"

namespace indexlibv2::framework {

class CommitOptions;

class MockTablet : public Tablet
{
public:
    MockTablet() : Tablet(TabletResource()) {}
    MockTablet(const TabletResource& resource) : Tablet(resource) {}

    MOCK_METHOD(Status, Open,
                (const IndexRoot&, const std::shared_ptr<config::ITabletSchema>&,
                 const std::shared_ptr<config::TabletOptions>&, const VersionCoord&),
                (override));
    MOCK_METHOD(Status, Reopen, (const ReopenOptions&, const VersionCoord&), (override));
    MOCK_METHOD(void, Close, (), (override));

    MOCK_METHOD(Status, Build, (const std::shared_ptr<document::IDocumentBatch>&), (override));
    MOCK_METHOD(bool, NeedCommit, (), (const, override));
    MOCK_METHOD((std::pair<Status, VersionMeta>), Commit, (const CommitOptions& commitOptions), (override));
    MOCK_METHOD(Status, CleanIndexFiles, (const std::vector<versionid_t>&), (override));

    MOCK_METHOD(Status, AlterTable, (const std::shared_ptr<config::ITabletSchema>&), (override));
    MOCK_METHOD(Status, Flush, (), (override));
    MOCK_METHOD(Status, Seal, (), (override));
    MOCK_METHOD((std::pair<Status, versionid_t>), ExecuteMerge, (versionid_t), ());

    MOCK_METHOD(std::shared_ptr<ITabletReader>, GetTabletReader, (), (const, override));
    MOCK_METHOD(TabletInfos*, GetTabletInfos, (), (const, override));
    MOCK_METHOD(std::shared_ptr<config::ITabletSchema>, GetTabletSchema, (), (const, override));
    MOCK_METHOD(std::shared_ptr<config::TabletOptions>, GetTabletOptions, (), (const, override));
    MOCK_METHOD(Status, ImportExternalFiles,
                (const std::string&, const std::vector<std::string>&, const std::shared_ptr<ImportExternalFileOptions>&,
                 Action action),
                (override));
};

typedef ::testing::NiceMock<MockTablet> NiceMockTablet;

} // namespace indexlibv2::framework

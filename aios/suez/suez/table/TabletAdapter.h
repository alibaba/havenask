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
#include "indexlib/framework/IndexRoot.h"
#include "suez/table/IIndexlibAdapter.h"

namespace indexlibv2 {
namespace framework {
class ITablet;
}
} // namespace indexlibv2

namespace indexlibv2 {
namespace config {
class TabletSchema;
}
} // namespace indexlibv2

namespace suez {

class TableWriter;

class TabletAdapter : public IIndexlibAdapter {
public:
    TabletAdapter(const std::shared_ptr<indexlibv2::framework::ITablet> &tablet,
                  std::string localRoot,
                  std::string remoteRoot,
                  const std::shared_ptr<indexlibv2::config::TabletSchema> &schema,
                  RoleType roleType);
    ~TabletAdapter();

public:
    TableStatus open(const PartitionProperties &properties, const TableVersion &tableVersion) override;
    TableStatus reopen(bool force, const TableVersion &tableVersion) override;

    void cleanIndexFiles(const std::set<IncVersion> &toKeepVersions) override;
    bool cleanUnreferencedIndexFiles(const std::set<std::string> &toKeepFiles) override;

    void reportAccessCounter(const std::string &name) const override;

    std::shared_ptr<indexlibv2::config::ITabletSchema> getSchema() const override;
    std::pair<TableVersion, SchemaVersion> getLoadedVersion() const override;

    TableBuilder *createBuilder(const build_service::proto::PartitionId &pid,
                                const build_service::workflow::RealtimeBuilderResource &rtResource,
                                const PartitionProperties &properties) const override;

    std::shared_ptr<SuezPartitionData>
    createSuezPartitionData(const PartitionId &pid, uint64_t totalPartitionCount, bool hasRt) const override;

public:
    std::shared_ptr<indexlibv2::framework::ITablet> getTablet() const;

private:
    std::shared_ptr<indexlibv2::framework::ITablet> _tablet;
    indexlibv2::framework::IndexRoot _indexRoot;
    std::shared_ptr<indexlibv2::config::TabletSchema> _schema;
    RoleType _roleType;
};

} // namespace suez

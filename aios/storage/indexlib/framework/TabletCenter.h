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

#include <mutex>

#include "autil/Log.h"
#include "autil/LoopThread.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/util/Singleton.h"

namespace indexlibv2::document {
class IDocumentParser;
} // namespace indexlibv2::document

namespace indexlibv2::framework {

class TabletCenter : public indexlib::util::Singleton<TabletCenter>
{
public:
    struct TabletResource {
        std::shared_ptr<ITablet> tabletPtr;
        std::shared_ptr<document::IDocumentParser> docParserPtr;
        void Reset()
        {
            tabletPtr.reset();
            docParserPtr.reset();
        }
    };

    class TabletResourceHandler
    {
    public:
        TabletResourceHandler(const std::shared_ptr<ITablet>& ptr) { _res.tabletPtr = ptr; }

        void Reset()
        {
            std::lock_guard<std::mutex> guard(_mutex);
            _res.Reset();
        }

        std::shared_ptr<ITablet> GetTabletPtr()
        {
            std::lock_guard<std::mutex> guard(_mutex);
            return _res.tabletPtr;
        }

        std::shared_ptr<document::IDocumentParser> GetDocumentParser()
        {
            std::lock_guard<std::mutex> guard(_mutex);
            return _res.docParserPtr;
        }

        void SetDocumentParser(const std::shared_ptr<document::IDocumentParser>& docParser)
        {
            std::lock_guard<std::mutex> guard(_mutex);
            _res.docParserPtr = docParser;
        }

    private:
        TabletResource _res;
        std::mutex _mutex;
    };

public:
    TabletCenter();
    ~TabletCenter();

    void Activate();
    void Deactivate();

    std::shared_ptr<TabletResourceHandler> RegisterTablet(const std::shared_ptr<ITablet>& tablet);
    void UnregisterTablet(ITablet* tablet);

    bool UpdateDocumentParser(const std::string& tabletName, const std::shared_ptr<document::IDocumentParser>& parser);

    void GetTabletByTabletName(const std::string& tabletName, std::vector<TabletResource>& handlers) const;
    void GetTabletBySchemaName(const std::string& schemaName, std::vector<TabletResource>& handlers) const;
    void GetAllTablets(std::vector<TabletResource>& handlers) const;

private:
    void EvictClosedTables();
    std::shared_ptr<TabletResourceHandler> FindTabletResourceHandleUnsafe(ITablet* tablet) const;

private:
    static const int64_t EVICT_LOOP_INTERVAL = 500 * 1000;

    typedef std::shared_ptr<TabletResourceHandler> TabletResourceHandlePtr;
    typedef std::vector<TabletResourceHandlePtr> TabletHandleVec;
    typedef std::vector<uint32_t> TabletIndexVec;
    typedef std::map<std::string, TabletIndexVec> Name2IndexMap;

    TabletHandleVec _tabletHandles;
    Name2IndexMap _tabletName2IdxMap;
    Name2IndexMap _schemaName2IdxMap;
    mutable std::mutex _mutex;
    autil::LoopThreadPtr _evictLoopThread;
    int64_t _lastTs = -1;
    bool _enable;

private:
    AUTIL_LOG_DECLARE();
};

} // namespace indexlibv2::framework

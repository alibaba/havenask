#ifndef INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEARCH_WORK_ITEM_H
#define INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEARCH_WORK_ITEM_H

#include <autil/WorkItem.h>
#include <autil/Lock.h>
#include <functional>
#include "aitheta_indexer/plugins/aitheta/common_define.h"
#include "aitheta_indexer/plugins/aitheta/index_segment_reader.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);

typedef std::function<void()> Function;

class SearchWorkItem : public autil::WorkItem {
 public:
    SearchWorkItem(Function func, std::shared_ptr<autil::TerminateNotifier> notifier, bool forceNotify = true);
    virtual ~SearchWorkItem();

    SearchWorkItem(const SearchWorkItem&) = delete;
    SearchWorkItem& operator=(const SearchWorkItem&) = delete;

 public:
    void process() override;
    void destroy() override { delete this; }
    void drop() override { delete this; }

 private:
    Function mClosure;
    std::shared_ptr<autil::TerminateNotifier> mNotifier;
    bool mForceNotify;
    bool mNotified;
    IE_LOG_DECLARE();
};

typedef std::unique_ptr<SearchWorkItem> SearchWorkItemPtr;

IE_NAMESPACE_END(aitheta_plugin);

#endif  // INDEXLIB_PLUGIN_PLUGINS_AITHETA_SEARCH_WORK_ITEM_H

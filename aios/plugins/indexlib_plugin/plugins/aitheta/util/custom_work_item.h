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
#ifndef INDEXLIB_PLUGIN_PLUGINS_AITHETA_CUSTOM_WORK_ITEM_H
#define INDEXLIB_PLUGIN_PLUGINS_AITHETA_CUSTOM_WORK_ITEM_H

#include <functional>
#include "autil/WorkItem.h"
#include "autil/Lock.h"
#include "autil/ThreadPool.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"

namespace indexlib {
namespace aitheta_plugin {

typedef std::function<void()> Function;

class CustomWorkItem : public autil::WorkItem {
 public:
    CustomWorkItem(const Function& func, std::shared_ptr<autil::TerminateNotifier> notifier = nullptr);
    virtual ~CustomWorkItem();

    CustomWorkItem(const CustomWorkItem&) = delete;
    CustomWorkItem& operator=(const CustomWorkItem&) = delete;

 public:
    void process() override;
    void destroy() override { delete this; }
    void drop() override { delete this; }

 public:
    static bool Run(CustomWorkItem* workItem, std::shared_ptr<autil::ThreadPool>& threadPool, bool isBlock = false);

 private:
    Function mClosure;
    std::shared_ptr<autil::TerminateNotifier> mNotifier;
    bool mNotified;
    IE_LOG_DECLARE();
};

typedef std::unique_ptr<CustomWorkItem> CustomWorkItemPtr;

}
}

#endif  // INDEXLIB_PLUGIN_PLUGINS_AITHETA_CUSTOM_WORK_ITEM_H

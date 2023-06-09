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
#include "indexlib_plugin/plugins/aitheta/util/custom_work_item.h"

namespace indexlib {
namespace aitheta_plugin {
using namespace std;
using namespace autil;

IE_LOG_SETUP(aitheta_plugin, CustomWorkItem);

CustomWorkItem::CustomWorkItem(const Function& closure, shared_ptr<TerminateNotifier> notifier)
    : mClosure(closure), mNotifier(notifier), mNotified(false) {}

CustomWorkItem::~CustomWorkItem() {
    if (!mNotified && mNotifier) {
        mNotifier->dec();
    }
}

void CustomWorkItem::process() {
    mClosure();
    if (mNotifier) {
        int32_t ret = mNotifier->dec();
        mNotified = true;
        if (unlikely(ret != 0)) {
            IE_LOG(ERROR, "failed to call dec in TerminateNotifier, error code[%d]", ret);
        }
    }
}

bool CustomWorkItem::Run(CustomWorkItem* workItem, std::shared_ptr<autil::ThreadPool>& threadPool, bool isBlock) {
    auto error = threadPool->pushWorkItem(workItem, isBlock);
    switch (error) {
        case autil::ThreadPool::ERROR_NONE: {
            IE_LOG(DEBUG, "workItem is pushed already");
            return true;
        }
        case autil::ThreadPool::ERROR_POOL_QUEUE_FULL: {
            IE_LOG(DEBUG, "failed to push,threadPool is full");
            delete workItem;
            return false;
        }
        default: {
            IE_LOG(ERROR, "unexpected error value[%d]", error);
            delete workItem;
            return false;
        }
    }
}

}
}

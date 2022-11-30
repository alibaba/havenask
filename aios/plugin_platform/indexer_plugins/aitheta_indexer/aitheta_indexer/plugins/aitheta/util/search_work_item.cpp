#include "aitheta_indexer/plugins/aitheta/util/search_work_item.h"

IE_NAMESPACE_BEGIN(aitheta_plugin);
using namespace std;
using namespace autil;

IE_LOG_SETUP(aitheta_plugin, SearchWorkItem);

SearchWorkItem::SearchWorkItem(Function closure, shared_ptr<TerminateNotifier> notifier, bool forceNotify)
    : mClosure(closure), mNotifier(notifier), mForceNotify(forceNotify), mNotified(false) {}

SearchWorkItem::~SearchWorkItem() {
    if (mForceNotify && !mNotified) {
        mNotifier->dec();
    }
}

void SearchWorkItem::process() {
    mClosure();
    int ret = mNotifier->dec();
    mNotified = true;
    if (unlikely(ret)) {
        IE_LOG(ERROR, "failed to call dec in TerminateNotifier, error code[%d]", ret);
    }
}

IE_NAMESPACE_END(aitheta_plugin);

#include <ha3/service/SessionWorkItem.h>

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, SessionWorkItem);

SessionWorkItem::SessionWorkItem(Session *session){ 
    assert(session);
    _session = session;
}

SessionWorkItem::~SessionWorkItem() {
}

void SessionWorkItem::process() {
    _session->processRequest();
}
void SessionWorkItem::destroy() {
    delete this;
}
void SessionWorkItem::drop() {
    _session->dropRequest();
    destroy();
}


END_HA3_NAMESPACE(service);


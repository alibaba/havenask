#ifndef ISEARCH_SESSIONWORKITEM_H
#define ISEARCH_SESSIONWORKITEM_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <autil/WorkItem.h>
#include <ha3/service/Session.h>

BEGIN_HA3_NAMESPACE(service);

class SessionWorkItem : public autil::WorkItem
{
public:
    SessionWorkItem(Session *session);
    ~SessionWorkItem();
private:
    SessionWorkItem(const SessionWorkItem &);
    SessionWorkItem& operator = (const SessionWorkItem &);
public:
    virtual void process();
    virtual void destroy();
    virtual void drop();
private:
    Session *_session;
private:
    HA3_LOG_DECLARE();
};

HA3_TYPEDEF_PTR(SessionWorkItem);

END_HA3_NAMESPACE(service);

#endif //ISEARCH_SESSIONWORKITEM_H

#ifndef ISEARCH_BS_LEADERCHECKERGUARD_H
#define ISEARCH_BS_LEADERCHECKERGUARD_H

class LeaderCheckerGuard {
public:
    LeaderCheckerGuard(WF_NS(worker_base)::LeaderElector *elector)
        : _elector(elector)
    {}
    ~LeaderCheckerGuard() {
        if (_elector) {
            _elector->stop();
        }
    }
    void done() {
        _elector = NULL;
    }
private:
    WF_NS(worker_base)::LeaderElector *_elector;
};

#endif //ISEARCH_BS_LEADERCHECKERGUARD_H

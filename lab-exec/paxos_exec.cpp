// Basic routines for Paxos implementation

#include "make_unique.h"
#include "paxmsg.h"
#include "paxserver.h"
#include "log.h"


//only performed by primary
//1. check duplicate request id
//2. log request
//3. propagate request

bool check(const std::unique_ptr<Paxlog::tup>& entry){
    //return true;
    return (*entry).executed;
}

void paxserver::execute_arg(const struct execute_arg& ex_arg) {
    if (!primary()){
        //send execute fail
        send_msg(ex_arg.src, std::make_unique<struct execute_fail>(vc_state.view.vid, vc_state.view.primary, ex_arg.rid));
        return;
    }
    if (paxlog.find_rid(nid, ex_arg.rid)) {
        return;
    }
    viewstamp_t new_vs;
    new_vs.vid = vc_state.view.vid;
    new_vs.ts = ts++;

    paxlog.log(ex_arg.src, ex_arg.rid, new_vs, ex_arg.request, get_serv_cnt(vc_state.view) ,net->now());
    std::set<node_id_t> servers = get_other_servers(vc_state.view);
    for(const auto& serv : servers) {
        send_msg(serv, std::make_unique<struct replicate_arg>(new_vs, ex_arg, paxlog.latest_exec() ));
    }
}


//
//1. check duplicate request id
//2. log request
//3. check committed if ok
//4. reply with replicate_res
void paxserver::replicate_arg(const struct replicate_arg& repl_arg) {

    std::vector<std::unique_ptr<struct Paxlog::tup>>::iterator entry = paxlog.begin();
    for(; entry != paxlog.end(); ++entry) {
        if ((*entry)->vs == repl_arg.vs) {
            return;
        }
    }
    //log request
    paxlog.log(vc_state.view.primary, repl_arg.arg.rid, repl_arg.vs, repl_arg.arg.request, get_serv_cnt(vc_state.view) ,net->now());

    entry = paxlog.begin();
    for(; entry != paxlog.end(); ++entry) {
        if (repl_arg.committed.ts >= (*entry)->vs.ts && paxlog.next_to_exec(entry)){
            //execute committed entries
            std::string result = paxop_on_paxobj(*entry);
            paxlog.execute(*entry);
        }
    }
    paxlog.trim_front(check);
    //reply to primary
    send_msg(repl_arg.src, std::make_unique<struct replicate_res>(repl_arg.vs));
}


// only for primary server
//1. increase the count on received view timestamp
//2. try to execute some command & sent replies to client
//3. try to propagate committed by accept_arg
void paxserver::replicate_res(const struct replicate_res& repl_res) {

    paxlog.incr_resp(repl_res.vs);
    //execute and reply to client
    std::vector<std::unique_ptr<struct Paxlog::tup>>::iterator entry = paxlog.begin();
    for(; entry != paxlog.end(); ++entry) {
        if (paxlog.next_to_exec(entry) && paxlog.get_tup((*entry)->vs)->resp_cnt*2 > get_serv_cnt(vc_state.view)) {
            std::string result = paxop_on_paxobj(*entry);
            paxlog.execute(*entry);
            send_msg((*entry)->src, std::make_unique<struct execute_success>(result, (*entry)->rid));
        }
    }
    paxlog.trim_front(check);
    std::set<node_id_t> servers = get_other_servers(vc_state.view);
    for(const auto& serv : servers) {
        send_msg(serv, std::make_unique<struct accept_arg>(paxlog.latest_exec()));
    }

}

void paxserver::accept_arg(const struct accept_arg& acc_arg) {

    std::vector<std::unique_ptr<struct Paxlog::tup>>::iterator entry = paxlog.begin();
    for(; entry != paxlog.end(); ++entry) {
        if (acc_arg.committed.ts >= (*entry)->vs.ts && paxlog.next_to_exec(entry)){
            //execute committed entries
            std::string result = paxop_on_paxobj(*entry);
            paxlog.execute(*entry);
        }
    }
    paxlog.trim_front(check);
}

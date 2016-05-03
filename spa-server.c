#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "spa_raft.h"

int sockfd;

struct {
  char *ip;
  short port;
} servers[] = { "127.0.0.1", 7001, "127.0.0.2", 7002, "127.0.0.3", 7003, NULL };

/** Callback for sending request vote messages */
int spa_send_requestvote(raft_server_t *raft, void *user_data,
                         raft_node_t *node, msg_requestvote_t *msg) {
  printf("Sending requestvote to node %d\n", raft_node_get_match_idx(node));

  spa_msg_t spa_msg;
  spa_msg.type = REQUEST_VOTE;
  spa_msg.content.requestvote = *msg;

  return sendto(sockfd, &msg, sizeof(msg), 0, (struct sockaddr *)user_data,
                sizeof(struct sockaddr_in));
}

/** Callback for sending appendentries messages */
int spa_send_appendentries(raft_server_t *raft, void *user_data,
                           raft_node_t *node, msg_appendentries_t *msg) {
  printf("Sending appendentries to node %d\n", raft_node_get_match_idx(node));
  
  spa_msg_t spa_msg;
  spa_msg.type = APPEND_ENTRIES;
  spa_msg.content.appendentries = *msg;

  return sendto(sockfd, &msg, sizeof(msg), 0, (struct sockaddr *)user_data,
                sizeof(struct sockaddr_in));
}

/** Callback for finite state machine application
 * Return 0 on success.
 * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
int spa_applylog(raft_server_t *raft, void *user_data, raft_entry_t *ety) {
  printf("%s: Apply Log: %d\n", (char *)user_data, *((int *)ety->data.buf));
  return 0;
}

/** Callback for persisting vote data
 * For safety reasons this callback MUST flush the change to disk. */
int spa_persist_vote(raft_server_t *raft, void *user_data, int node) {
  printf("%s: Persisting vote for node %d\n", (char *) user_data, node);
  
  return 0;
}

/** Callback for persisting term data
 * For safety reasons this callback MUST flush the change to disk. */
int spa_persist_term(raft_server_t *raft, void *user_data, int node) {
  printf("%s: Persisting term for node %d\n", (char *) user_data, node);
  return 0;
}

/** Callback for adding an entry to the log
 * For safety reasons this callback MUST flush the change to disk.
 * Return 0 on success.
 * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
int spa_log_offer(raft_server_t *raft, void *user_data, raft_entry_t *entry,
                  int entry_idx) {
  printf("%s: Log Offer [%d]: %d\n", (char *)user_data, entry_idx,
         *((int *)entry->data.buf));
  return 0;
}

/** Callback for removing the oldest entry from the log
 * For safety reasons this callback MUST flush the change to disk.
 * @note If memory was malloc'd in log_offer then this should be the right
 *  time to free the memory. */
int spa_log_poll(raft_server_t *raft, void *user_data, raft_entry_t *entry,
                 int entry_idx) {
  printf("%s: Log Poll [%d]: %d\n", (char *)user_data, entry_idx,
         *((int *)entry->data.buf));
  return 0;
}

/** Callback for removing the youngest entry from the log
 * For safety reasons this callback MUST flush the change to disk.
 * @note If memory was malloc'd in log_offer then this should be the right
 *  time to free the memory. */
int spa_log_pop(raft_server_t *raft, void *user_data, raft_entry_t *entry,
                int entry_idx) {
  printf("%s: Log Pop [%d]: %d\n", (char *)user_data, entry_idx,
         *((int *)entry->data.buf));
  return 0;
}

/** Callback for detecting when a non-voting node has sufficient logs. */
void spa_node_has_sufficient_logs(raft_server_t *raft, void *user_data,
                                  raft_node_t *node) {}

/** Callback for catching debugging log messages
 * This callback is optional */
void spa_log(raft_server_t *raft, raft_node_t *node, void *user_data,
             const char *buf) {
  printf("%s: Node %d: %s\n", (char *)user_data,
         node ? raft_node_get_match_idx(node) : -1, buf);
}

int main(int argc, char **argv) {
  assert(argc == 2);
  int node_id = atoi(argv[1]);

  assert((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) >= 0);

#ifndef ENABLE_KLEE
  struct timeval tv = { 1, 0 };
  setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv,
             sizeof(struct timeval));
#endif

  raft_server_t *raft = raft_new();

  raft_cbs_t callbacks = { spa_send_requestvote, spa_send_appendentries,
                           spa_applylog, spa_persist_vote, spa_persist_term,
                           spa_log_offer, spa_log_poll, spa_log_pop,
                           spa_node_has_sufficient_logs, spa_log };
  raft_set_callbacks(raft, &callbacks, "Raft Server");

  int id;
  for (id = 0; servers[id].ip; id++) {
    struct sockaddr_in *srvaddr = malloc(sizeof(struct sockaddr_in));
    bzero(srvaddr, sizeof(struct sockaddr_in));
    srvaddr->sin_family = AF_INET;
    inet_pton(AF_INET, servers[id].ip, &srvaddr->sin_addr);
    srvaddr->sin_port = htons(servers[id].port);

    raft_add_node(raft, srvaddr, id, id == node_id);

    if (id==node_id){
    assert(bind(sockfd, (struct sockaddr *)srvaddr, sizeof(struct sockaddr_in)) == 0);
    printf("Listening on %s:%d\n", servers[id].ip, servers[id].port);
    }
  }

  while (1) {
    spa_msg_t spa_msg;
    struct sockaddr_in srvaddr;
    socklen_t socklen = sizeof(srvaddr);
    ssize_t resplen = recvfrom(sockfd, &spa_msg, sizeof(spa_msg), 0,
                               (struct sockaddr *)&srvaddr, &socklen);
    assert(socklen == sizeof(srvaddr));

    if (resplen >= 0) {
      assert(resplen == sizeof(spa_msg));

#ifndef ENABLE_KLEE
      char srvstr[INET_ADDRSTRLEN + 1 + 5 + 1];
      inet_ntop(AF_INET, &srvaddr.sin_addr.s_addr, srvstr, socklen);
      size_t pos = strlen(srvstr);
      srvstr[pos++] = ':';
      snprintf(&srvstr[pos], 6, "%d", ntohs(srvaddr.sin_port));

      printf("Incoming message from %s, type = %d\n", srvstr, spa_msg.type);
#endif

      int node_idx = ntohs(srvaddr.sin_port) - servers[0].port;
      assert(node_idx >= 0 && node_idx < raft_get_num_nodes(raft));
      assert(ntohs(srvaddr.sin_port) == servers[node_idx].port);
      raft_node_t *node = raft_get_node(raft, node_idx);
      assert(node);

      spa_msg_t response;
      switch (spa_msg.type) {
      case ENTRY:
        response.type = ENTRY_RESPONSE;
        int result = raft_recv_entry(raft, &spa_msg.content.entry,
                                     &response.content.entry_response);
        if (result == RAFT_ERR_NOT_LEADER) {
          raft_node_t *leader = raft_get_current_leader_node(raft);
          assert(leader);
          struct sockaddr_in *leader_addr = raft_node_get_udata(leader);
          assert(leader_addr);
          response.type = ENTRY_REDIRECT;
          response.content.entry_redirect = *leader_addr;
        } else {
          assert(result == 0);
        }
        break;
      case REQUEST_VOTE:
        response.type = REQUEST_VOTE_RESPONSE;
        assert(raft_recv_requestvote(raft, node, &spa_msg.content.requestvote,
                                     &response.content.requestvote_response) ==
               0);
        break;
      case REQUEST_VOTE_RESPONSE:
        assert(raft_recv_requestvote_response(
            raft, node, &spa_msg.content.requestvote_response) == 0);
        break;
      case APPEND_ENTRIES:
        response.type = APPEND_ENTRIES_RESPONSE;
        assert(raft_recv_appendentries(
            raft, node, &spa_msg.content.appendentries,
            &response.content.appendentries_response) == 0);
        break;
      case APPEND_ENTRIES_RESPONSE:
        assert(raft_recv_appendentries_response(
            raft, node, &spa_msg.content.appendentries_response) == 0);
        break;
      default:
        assert(0);
        break;
      }
    } else {
      //       usleep(100000);
      assert(raft_periodic(raft, 100) == 0);
    }
  }

  raft_free(raft);
  return 0;
}

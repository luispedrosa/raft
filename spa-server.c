#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <assert.h>

#include "spa_raft.h"

spa_state_t state = 1;

int sockfd;

struct {
  char *ip;
  short port;
} servers[] = { "127.0.0.1", 7001, "127.0.0.2", 7002, "127.0.0.3", 7003, NULL };

/** Callback for sending request vote messages */
int spa_send_requestvote(raft_server_t *raft, void *user_data,
                         raft_node_t *node, msg_requestvote_t *msg) {
#ifndef ENABLE_KLEE
  char nodestr[INET_ADDRSTRLEN + 1 + 5 + 1];
  inet_ntop(AF_INET,
            &((struct sockaddr_in *)raft_node_get_udata(node))->sin_addr.s_addr,
            nodestr, sizeof(nodestr));
  int pos = strlen(nodestr);
  nodestr[pos++] = ':';
  snprintf(&nodestr[pos], 6, "%d",
           ntohs(((struct sockaddr_in *)raft_node_get_udata(node))->sin_port));
  printf("%s: Sending requestvote to node %d (%s)\n", (char *)user_data,
         raft_node_get_id(node), nodestr);
#endif

  spa_msg_t spa_msg;
  spa_msg.type = REQUEST_VOTE;
  spa_msg.content.requestvote = *msg;

  return sendto(sockfd, &spa_msg, sizeof(spa_msg), 0,
                (struct sockaddr *)raft_node_get_udata(node),
                sizeof(struct sockaddr_in));
}

/** Callback for sending appendentries messages */
int spa_send_appendentries(raft_server_t *raft, void *user_data,
                           raft_node_t *node, msg_appendentries_t *msg) {
#ifndef ENABLE_KLEE
  char nodestr[INET_ADDRSTRLEN + 1 + 5 + 1];
  inet_ntop(AF_INET,
            &((struct sockaddr_in *)raft_node_get_udata(node))->sin_addr.s_addr,
            nodestr, sizeof(nodestr));
  int pos = strlen(nodestr);
  nodestr[pos++] = ':';
  snprintf(&nodestr[pos], 6, "%d",
           ntohs(((struct sockaddr_in *)raft_node_get_udata(node))->sin_port));
  printf("%s: Sending appendentries to node %d (%s)\n", (char *)user_data,
         raft_node_get_id(node), nodestr);
#endif

  spa_msg_t spa_msg;
  spa_msg.type = APPEND_ENTRIES;
  spa_msg.content.appendentries = *msg;

  return sendto(sockfd, &spa_msg, sizeof(spa_msg), 0,
                (struct sockaddr *)raft_node_get_udata(node),
                sizeof(struct sockaddr_in));
}

/** Callback for finite state machine application
 * Return 0 on success.
 * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
int spa_applylog(raft_server_t *raft, void *user_data, raft_entry_t *ety) {
  printf("%s: Apply Log: %d\n", (char *)user_data,
         *((spa_state_t *)ety->data.buf));
  state = *((spa_state_t *)ety->data.buf);
  return 0;
}

/** Callback for persisting vote data
 * For safety reasons this callback MUST flush the change to disk. */
int spa_persist_vote(raft_server_t *raft, void *user_data, int node) {
  printf("%s: Persisting vote for node %d\n", (char *)user_data, node);

  return 0;
}

/** Callback for persisting term data
 * For safety reasons this callback MUST flush the change to disk. */
int spa_persist_term(raft_server_t *raft, void *user_data, int node) {
  printf("%s: Persisting term for node %d\n", (char *)user_data, node);
  return 0;
}

/** Callback for adding an entry to the log
 * For safety reasons this callback MUST flush the change to disk.
 * Return 0 on success.
 * Return RAFT_ERR_SHUTDOWN if you want the server to shutdown. */
int spa_log_offer(raft_server_t *raft, void *user_data, raft_entry_t *entry,
                  int entry_idx) {
  printf("%s: Log Offer [%d]: %d\n", (char *)user_data, entry_idx,
         *((spa_state_t *)entry->data.buf));
  return 0;
}

/** Callback for removing the oldest entry from the log
 * For safety reasons this callback MUST flush the change to disk.
 * @note If memory was malloc'd in log_offer then this should be the right
 *  time to free the memory. */
int spa_log_poll(raft_server_t *raft, void *user_data, raft_entry_t *entry,
                 int entry_idx) {
  printf("%s: Log Poll [%d]: %d\n", (char *)user_data, entry_idx,
         *((spa_state_t *)entry->data.buf));
  return 0;
}

/** Callback for removing the youngest entry from the log
 * For safety reasons this callback MUST flush the change to disk.
 * @note If memory was malloc'd in log_offer then this should be the right
 *  time to free the memory. */
int spa_log_pop(raft_server_t *raft, void *user_data, raft_entry_t *entry,
                int entry_idx) {
  printf("%s: Log Pop [%d]: %d\n", (char *)user_data, entry_idx,
         *((spa_state_t *)entry->data.buf));
  return 0;
}

/** Callback for detecting when a non-voting node has sufficient logs. */
void spa_node_has_sufficient_logs(raft_server_t *raft, void *user_data,
                                  raft_node_t *node) {}

/** Callback for catching debugging log messages
 * This callback is optional */
void spa_log(raft_server_t *raft, raft_node_t *node, void *user_data,
             const char *buf) {
#ifndef ENABLE_KLEE
  char nodestr[INET_ADDRSTRLEN + 1 + 5 + 1];
  if (node) {
    inet_ntop(AF_INET, &((struct sockaddr_in *)raft_node_get_udata(node))
                           ->sin_addr.s_addr,
              nodestr, sizeof(nodestr));
    int pos = strlen(nodestr);
    nodestr[pos++] = ':';
    snprintf(
        &nodestr[pos], 6, "%d",
        ntohs(((struct sockaddr_in *)raft_node_get_udata(node))->sin_port));
  }
  printf("%s: Node %s: %s\n", (char *)user_data, node ? nodestr : "-", buf);
#endif
}

int main(int argc, char **argv) {
#ifndef ENABLE_KLEE
  setvbuf(stdout, NULL, _IONBF, 0);
#endif

  assert(argc == 2);
  int node_id = atoi(argv[1]);

  assert((sockfd = socket(AF_INET, SOCK_DGRAM, 0)) >= 0);

#ifndef ENABLE_KLEE
  struct timeval tv = { 0 /* sec */, 100000 /* usec */ };
  setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv,
             sizeof(struct timeval));
#endif

  raft_server_t *raft = raft_new();

  raft_cbs_t callbacks = { spa_send_requestvote, spa_send_appendentries,
                           spa_applylog, NULL/*spa_persist_vote*/, NULL/*spa_persist_term*/,
                           NULL/*spa_log_offer*/, NULL/*spa_log_poll*/, NULL/*spa_log_pop*/,
                           NULL/*spa_node_has_sufficient_logs*/, spa_log };
  char server_name[100];
  snprintf(server_name, sizeof(server_name), "Raft Server %d", node_id);
  raft_set_callbacks(raft, &callbacks, server_name);

  int id;
  for (id = 0; servers[id].ip; id++) {
    struct sockaddr_in *srvaddr = malloc(sizeof(struct sockaddr_in));
    bzero(srvaddr, sizeof(struct sockaddr_in));
    srvaddr->sin_family = AF_INET;
    inet_pton(AF_INET, servers[id].ip, &srvaddr->sin_addr);
    srvaddr->sin_port = htons(servers[id].port);

    raft_add_node(raft, srvaddr, id, id == node_id);

    if (id == node_id) {
      assert(bind(sockfd, (struct sockaddr *)srvaddr,
                  sizeof(struct sockaddr_in)) == 0);
      printf("%s: Listening on %s:%d\n", server_name, servers[id].ip,
             servers[id].port);
    }
  }

  msg_entry_response_t *entry_response = NULL;
  struct sockaddr_in cliaddr;

  while (1) {
    spa_msg_t spa_msg;
    struct sockaddr_in srvaddr;
    bzero(&srvaddr, sizeof(srvaddr));
    socklen_t socklen = sizeof(srvaddr);
    ssize_t resplen = recvfrom(sockfd, &spa_msg, sizeof(spa_msg), 0,
                               (struct sockaddr *)&srvaddr, &socklen);
    assert(socklen == sizeof(srvaddr));

    if (resplen >= 0) {
      int node_idx = -1, i;
      raft_node_t *node = NULL;
      for (i = 0; i < raft_get_num_nodes(raft); i++) {
        if (memcmp(raft_node_get_udata(raft_get_node(raft, i)), &srvaddr,
                   sizeof(srvaddr)) == 0) {
          node_idx = i;
          node = raft_get_node(raft, i);
          break;
        }
      }

#ifndef ENABLE_KLEE
      char srvstr[INET_ADDRSTRLEN + 1 + 5 + 1];
      inet_ntop(AF_INET, &srvaddr.sin_addr.s_addr, srvstr, sizeof(srvstr));
      size_t pos = strlen(srvstr);
      srvstr[pos++] = ':';
      snprintf(&srvstr[pos], 6, "%d", ntohs(srvaddr.sin_port));

      printf("%s: Incoming message from %s (node %d), type = %d\n", server_name,
             srvstr, node_idx, spa_msg.type);
#endif

      assert(resplen == sizeof(spa_msg));

      switch (spa_msg.type) {
      case GET: {
        spa_msg_t response;
        if (raft_is_leader(raft)) {
          printf("%s: Received GET request. state == %d\n", server_name, state);
          response.type = GET_RESPONSE;
          response.content.get_response = state;
        } else {
          raft_node_t *leader = raft_get_current_leader_node(raft);
          if (!leader) {
            leader = raft_get_node(raft, 0);
          }
          printf("%s: Received GET request. Redirecting to node %d\n",
                 server_name, raft_node_get_id(leader));
          struct sockaddr_in *leader_addr = raft_node_get_udata(leader);
          assert(leader_addr);
          response.type = REDIRECT;
          response.content.redirect = *leader_addr;
        }

        sendto(sockfd, &response, sizeof(spa_msg_t), 0,
               (struct sockaddr *)&srvaddr, sizeof(srvaddr));
      } break;
      case SET: {
        if (entry_response) {
          raft_node_t *leader = raft_get_current_leader_node(raft);
          if (!leader) {
            leader = raft_get_node(raft, 0);
          }
          printf("%s: Received SET request in the middle of a transaction. "
                 "Redirecting to node %d\n",
                 server_name, raft_node_get_id(leader));
          struct sockaddr_in *leader_addr = raft_node_get_udata(leader);
          assert(leader_addr);

          spa_msg_t response;
          response.type = REDIRECT;
          response.content.redirect = *leader_addr;

          sendto(sockfd, &response, sizeof(spa_msg_t), 0,
                 (struct sockaddr *)&srvaddr, sizeof(srvaddr));
        }

        raft_entry_t entry;
        entry.id = 1;
        entry.term = 1;
        entry.type = RAFT_LOGTYPE_NORMAL;
        entry.data.buf = &spa_msg.content.set;
        entry.data.len = sizeof(state);

        entry_response = malloc(sizeof(msg_entry_response_t));
        int result = raft_recv_entry(raft, &entry, entry_response);

        if (result == RAFT_ERR_NOT_LEADER) {
          free(entry_response);
          entry_response = NULL;

          raft_node_t *leader = raft_get_current_leader_node(raft);
          if (!leader) {
            leader = raft_get_node(raft, 0);
          }
          printf("%s: Received SET request but not leader. Redirecting to node "
                 "%d\n",
                 server_name, raft_node_get_id(leader));
          struct sockaddr_in *leader_addr = raft_node_get_udata(leader);
          assert(leader_addr);

          spa_msg_t response;
          response.type = REDIRECT;
          response.content.redirect = *leader_addr;

          sendto(sockfd, &response, sizeof(spa_msg_t), 0,
                 (struct sockaddr *)&srvaddr, sizeof(srvaddr));
        } else {
          assert(result == 0);
          printf("%s: Received SET request. Waiting for commit\n", server_name);
          cliaddr = srvaddr;
        }
      } break;
      case REQUEST_VOTE: {
        assert(node);
        spa_msg_t response;
        response.type = REQUEST_VOTE_RESPONSE;
        assert(raft_recv_requestvote(raft, node, &spa_msg.content.requestvote,
                                     &response.content.requestvote_response) ==
               0);

        sendto(sockfd, &response, sizeof(spa_msg_t), 0,
               (struct sockaddr *)&srvaddr, sizeof(srvaddr));
      } break;
      case REQUEST_VOTE_RESPONSE: {
        assert(node);
        assert(raft_recv_requestvote_response(
            raft, node, &spa_msg.content.requestvote_response) == 0);
      } break;
      case APPEND_ENTRIES: {
        assert(node);
        spa_msg_t response;
        response.type = APPEND_ENTRIES_RESPONSE;
        assert(raft_recv_appendentries(
            raft, node, &spa_msg.content.appendentries,
            &response.content.appendentries_response) == 0);

        sendto(sockfd, &response, sizeof(spa_msg_t), 0,
               (struct sockaddr *)&srvaddr, sizeof(srvaddr));
      } break;
      case APPEND_ENTRIES_RESPONSE: {
        assert(node);
        assert(raft_recv_appendentries_response(
            raft, node, &spa_msg.content.appendentries_response) == 0);
      } break;
      default: {
        assert(0);
      } break;
      }

    } else if (entry_response &&
               raft_msg_entry_response_committed(raft, entry_response)) {
      printf("%s: SET request committed\n", server_name);

      free(entry_response);
      entry_response = NULL;

      spa_msg_t response;
      response.type = SET_RESPONSE;

      sendto(sockfd, &response, sizeof(spa_msg_t), 0,
             (struct sockaddr *)&cliaddr, sizeof(cliaddr));
    } else {

      //       usleep(100000);
      assert(raft_periodic(raft, 100) == 0);
    }
  }

  raft_free(raft);
  return 0;
}

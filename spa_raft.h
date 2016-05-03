#include "include/raft.h"
#include <netinet/in.h>

typedef uint8_t spa_state_t;

typedef enum {
  GET,
  GET_RESPONSE,
  ENTRY,
  ENTRY_RESPONSE,
  ENTRY_REDIRECT,
  REQUEST_VOTE,
  REQUEST_VOTE_RESPONSE,
  APPEND_ENTRIES,
  APPEND_ENTRIES_RESPONSE,
} spa_msg_type_t;

typedef struct {
  spa_msg_type_t type;
  union {
    spa_state_t get_response;
    msg_entry_t entry;
    msg_entry_response_t entry_response;
    struct sockaddr_in entry_redirect;
    msg_requestvote_t requestvote;
    msg_requestvote_response_t requestvote_response;
    msg_appendentries_t appendentries;
    msg_appendentries_response_t appendentries_response;
  } content;
} spa_msg_t;
#include "include/raft.h"
#include <netinet/in.h>

typedef uint8_t spa_state_t;

typedef enum {
  GET,
  GET_RESPONSE,
  SET,
  SET_RESPONSE,
  REDIRECT,
  REQUEST_VOTE,
  REQUEST_VOTE_RESPONSE,
  APPEND_ENTRIES,
  APPEND_ENTRIES_RESPONSE,
} spa_msg_type_t;

typedef struct {
  spa_msg_type_t type;
  union {
    spa_state_t get_response;
    spa_state_t set;
    struct sockaddr_in redirect;
    msg_requestvote_t requestvote;
    msg_requestvote_response_t requestvote_response;
    msg_appendentries_t appendentries;
    msg_appendentries_response_t appendentries_response;
  } content;
} spa_msg_t;
#ifndef ANALYTICS_H
#define ANALYTICS_H

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <assert.h>
#include <sys/types.h>
#include "time_utils.h"
#include "data.h"
#include "ht.h"

typedef struct key {
    char *key;
    struct key *next;
} my_key_t;

typedef struct key_list {
    my_key_t *head_key;
    my_key_t *tail_key;
} key_list;

typedef struct sentiment_node {
    char key[HOUR_STR_LEN];
    long double sentiment;
} sentiment_node;

typedef struct count_node {
    char key[HOUR_STR_LEN];
    int count;
} count_node;

typedef struct items {
    sentiment_node *happy_hour;
    sentiment_node *happy_day;
    count_node *active_hour;
    count_node *active_day;
} items;

my_key_t *alloc_one_key(size_t key_size);

key_list *alloc_key_list();

key_list **all_keys_setup();

HashTable **all_ht_setup();

void destroy_all_ht(HashTable *hts); /** TODO: Implement destroying ht*/
void insert_key(key_list *key_list, char *key, size_t size);

void free_key_list(key_list **key_list);

void process_tweet_data(HashTable **ht, key_list **keys, data_struct *data);

void consolidate_child_data(HashTable *ht, key_list *keys, int score_type, void *node, int list_size, int starting_index);

ret_struct *process_tweets(data_struct *data);

items *all_ht_to_lists(HashTable **hash_tables, key_list **key_lists);

void *ht_to_list(HashTable *ht, key_list *key_list, int item_type);

int update_ht_and_key(HashTable *ht, key_list *key_list, char *key, void *value, int data_type);

void find_most(HashTable *ht, key_list *key_list, char *leading_time, int data_type);

#endif
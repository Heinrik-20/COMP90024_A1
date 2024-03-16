#include "analytics.h"

my_key_t *alloc_one_key(size_t key_size) {
    my_key_t *new = (my_key_t *)malloc(sizeof(my_key_t));
    assert(new);
    
    new->key = (char *)malloc(sizeof(char) * key_size);
    assert(new->key);

    new->next = NULL;
    return new;
}

key_list *alloc_key_list() {
    key_list *new = (key_list *)malloc(sizeof(key_list));
    assert(new);
    new->head_key = NULL;
    new->tail_key = NULL;
    return new;
}

void insert_key(key_list *key_list, char *key, size_t size) {
    if (key_list->head_key) {
        my_key_t *new = alloc_one_key(size);
        memcpy(new->key, key, size);
        key_list->tail_key->next = new;
        key_list->tail_key = new;
    } else {
        my_key_t *new = alloc_one_key(size);
        memcpy(new->key, key, size);
        key_list->head_key = new;
        key_list->tail_key = new;
    }
}

void free_key_list(key_list **key_list) {
    my_key_t *curr = (*key_list)->head_key;
    while (curr) {
        free(curr->key);
        curr->key = NULL;
        curr = curr->next;
    }
    return;
}

ret_struct *process_tweets(data_struct *data) {
    
    HashTable happy_hour;
    HashTable happy_day;
    HashTable active_hour;
    HashTable active_day;

    ht_setup(&happy_hour, HOUR_STR_LEN, sizeof(long double), HT_MINIMUM_CAPACITY);
    ht_setup(&happy_day, DATE_STR_LEN, sizeof(long double), HT_MINIMUM_CAPACITY);
    ht_setup(&active_hour, HOUR_STR_LEN, sizeof(int), HT_MINIMUM_CAPACITY);
    ht_setup(&active_day, DATE_STR_LEN, sizeof(int), HT_MINIMUM_CAPACITY);

    key_list *happy_hour_keys = alloc_key_list(); // TODO: free mem
    key_list *happy_day_keys = alloc_key_list();
    key_list *active_hour_keys = alloc_key_list();
    key_list *active_day_keys = alloc_key_list();

    int most_active_day_count = 1, most_active_hour_count = 1;

    char *most_happy_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
    char *most_happy_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
    char *most_active_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
    char *most_active_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);

    data_struct *curr = data;

    while (curr) {
        char **time_strings = time_struct_to_str(curr->time);
        if (curr == data) {
            if (curr->sentiment > -9990) {
                update_ht_and_key(&happy_hour, happy_hour_keys, time_strings[0], &(curr->sentiment), 1);
                update_ht_and_key(&happy_day, happy_day_keys, time_strings[1], &(curr->sentiment), 1);
            }
            update_ht_and_key(&active_hour, active_hour_keys, time_strings[0], &most_active_hour_count, 0);
            update_ht_and_key(&active_day, active_day_keys, time_strings[1], &most_active_day_count, 0);
        } else {
            if (curr->sentiment > -9990) {
                update_ht_and_key(&happy_hour, happy_hour_keys, time_strings[0], &(curr->sentiment), 1);
                update_ht_and_key(&happy_day, happy_day_keys, time_strings[1], &(curr->sentiment), 1);
            }
            update_ht_and_key(&active_hour, active_hour_keys, time_strings[0], NULL, 0);
            update_ht_and_key(&active_day, active_day_keys, time_strings[1], NULL, 0);
        }

        free(time_strings[0]);
        free(time_strings[1]);
        time_strings[0] = NULL;
        time_strings[1] = NULL;

        free(time_strings);
        time_strings = NULL;

        curr = curr->next;
    }

    find_most(&happy_hour, happy_hour_keys, most_happy_hour, 1);
    find_most(&happy_day, happy_day_keys, most_happy_day, 1);
    find_most(&active_hour, active_hour_keys, most_active_hour, 0);
    find_most(&active_day, active_day_keys, most_active_day, 0);

    ret_struct *ret = alloc_ret_struct();
    memcpy(ret->happy_hour_time, most_happy_hour, HOUR_STR_LEN);
    memcpy(ret->happy_day_time, most_happy_day, DATE_STR_LEN);
    memcpy(ret->active_hour_time, most_active_hour, HOUR_STR_LEN);
    memcpy(ret->active_day_time, most_active_day, DATE_STR_LEN);

    ret->happy_hour_sentiment = *(long double *)ht_lookup(&happy_hour, ret->happy_hour_time);
    ret->happy_day_sentiment = *(long double *)ht_lookup(&happy_day, ret->happy_day_time);
    ret->active_hour_count = *(int *)ht_lookup(&active_hour, ret->active_hour_time);
    ret->active_day_count = *(int *)ht_lookup(&active_day, ret->active_day_time);

    free(most_happy_hour);
    free(most_happy_day);
    free(most_active_hour);
    free(most_active_day);
    
    ht_destroy(&happy_hour);
    ht_destroy(&happy_day);
    ht_destroy(&active_hour);
    ht_destroy(&active_day);

    free_key_list(&happy_hour_keys);
    free_key_list(&happy_day_keys);
    free_key_list(&active_hour_keys);
    free_key_list(&active_day_keys);

    return ret;

}

int update_ht_and_key(HashTable *ht, key_list *key_list, char *key, void *value, int data_type) { // data_type = 1 -> sentiment, 0 -> count;
    if (data_type) {
        long double *sentiment = (long double *) ht_lookup(ht, (void *)key);
        if (sentiment) { // Key is found in Hashtable, no need to insert to list
            long double temp = *sentiment;
            temp += *(long double *)value;
            ht_insert(ht, (void *)key, &temp);
        } else {
            // Key is not found in Hashtable
            ht_insert(ht, (void *)key, value);
            insert_key(key_list, key, ht->key_size);
        }

        return HT_SUCCESS;

    } else {
        int *count = (int *) ht_lookup(ht, (void *)key);
        int temp = 1;
        if (count) { // Key is found in Hashtable, no need to insert to list
            temp = *count;
            temp += 1;
            ht_insert(ht, (void *)key, &temp);
        } else { 
            // Key is not found in Hashtable
            ht_insert(ht, (void *)key, &temp);
            insert_key(key_list, key, ht->key_size);
        }

        return HT_SUCCESS;
    }

    return HT_ERROR;
}

void find_most(HashTable *ht, key_list *key_list, char *leading_time, int data_type) { // data_type = 1 -> sentiment, 0 -> count;
    char *max_time = key_list->head_key->key;
    void *max_val = ht_lookup(ht, key_list->head_key->key);

    my_key_t *curr = key_list->head_key->next;
    while (curr) {
        void *val = ht_lookup(ht, curr->key);
        if (data_type) {
            if (*(long double *)val > *(long double *)max_val) {
                max_val = val;
                max_time = curr->key;
            }
        } else {
            if (*(int *)val > *(int *)max_val) {
                max_val = val;
                max_time = curr->key;
            }
        }

        curr = curr->next;
    }

    memcpy(leading_time, max_time, ht->key_size);
    return;
}

#include <mpi.h>
#include <stdio.h>
#include "utils/time_utils.h"
#include "utils/data.h"
#include "utils/analytics.h"
#include "utils/ht.h"

#define MAX_READS 10000

int main(int argc, char** argv) {

    size_t size = 0;
    FILE *fp = fopen(argv[1], "r"); // We will need to make sure of a certain format to use argv[1], please run in this format: mpirun -np 1 ./build/main ../data/twitter-1mb.json

    char *starting_line = NULL;
    getline_clean(&starting_line, &size, fp);
    free(starting_line);
    starting_line = NULL;

    HashTable happy_hour;
    HashTable happy_day;
    HashTable active_hour;
    HashTable active_day;

    ht_setup(&happy_hour, HOUR_STR_LEN, sizeof(long double), HT_MINIMUM_CAPACITY);
    ht_setup(&happy_day, DATE_STR_LEN, sizeof(long double), HT_MINIMUM_CAPACITY);
    ht_setup(&active_hour, HOUR_STR_LEN, sizeof(int), HT_MINIMUM_CAPACITY);
    ht_setup(&active_day, DATE_STR_LEN, sizeof(int), HT_MINIMUM_CAPACITY);

    key_list *happy_hour_keys = alloc_key_list();
    key_list *happy_day_keys = alloc_key_list();
    key_list *active_hour_keys = alloc_key_list();
    key_list *active_day_keys = alloc_key_list();

    int most_active_day_count = 1, most_active_hour_count = 1;

    char *most_happy_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
    char *most_happy_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
    char *most_active_hour = (char *) malloc(sizeof(char) * HOUR_STR_LEN);
    char *most_active_day = (char *) malloc(sizeof(char) * DATE_STR_LEN);
    
    while (getline_clean(&starting_line, &size, fp) != (-1)) {
        data_struct *data = read_data_char(starting_line, size);
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
        free_data_struct_list(&data);
    }

    find_most(&happy_hour, happy_hour_keys, most_happy_hour, 1);
    find_most(&happy_day, happy_day_keys, most_happy_day, 1);
    find_most(&active_hour, active_hour_keys, most_active_hour, 0);
    find_most(&active_day, active_day_keys, most_active_day, 0);

    printf("Most Happy Hour: %s, sentiment: %.2LF\n", most_happy_hour, *(long double *)ht_lookup(&happy_hour, most_happy_hour));
    printf("Most Happy Day: %s, sentiment: %.2LF\n", most_happy_day, *(long double *)ht_lookup(&happy_day, most_happy_day));
    printf("Most Active Hour: %s, count: %d\n", most_active_hour, *(int *)ht_lookup(&active_hour, most_active_hour));
    printf("Most Active Day: %s, count: %d\n", most_active_day, *(int *)ht_lookup(&active_day, most_active_day));

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

    fclose(fp);
}